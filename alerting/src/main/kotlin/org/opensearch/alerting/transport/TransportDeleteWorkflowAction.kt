/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.join.ScoreMode
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.AlertingPluginInterface
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorRequest
import org.opensearch.commons.alerting.action.DeleteMonitorResponse
import org.opensearch.commons.alerting.action.DeleteWorkflowRequest
import org.opensearch.commons.alerting.action.DeleteWorkflowResponse
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Schedule
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.WorkflowInput
import org.opensearch.commons.alerting.util.IndexUtils
import org.opensearch.commons.alerting.util.instant
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant
import java.util.Locale

private val log = LogManager.getLogger(TransportIndexMonitorAction::class.java)

class TransportDeleteWorkflowAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ActionRequest, DeleteWorkflowResponse>(
    AlertingActions.DELETE_WORKFLOW_ACTION_NAME, transportService, actionFilters, ::DeleteWorkflowRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<DeleteWorkflowResponse>) {
        val transformedRequest = request as? DeleteWorkflowRequest
            ?: recreateObject(request) { DeleteWorkflowRequest(it) }

        val user = readUserFromThreadContext(client)
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, transformedRequest.workflowId)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        GlobalScope.launch(Dispatchers.IO + CoroutineName("DeleteWorkflowAction")) {
            DeleteWorkflowHandler(client, actionListener, deleteRequest, transformedRequest.deleteUnderlyingMonitors, user, transformedRequest.workflowId).resolveUserAndStart()
        }
    }

    inner class DeleteWorkflowHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteWorkflowResponse>,
        private val deleteRequest: DeleteRequest,
        private val deleteUnderlyingMonitors: Boolean?,
        private val user: User?,
        private val workflowId: String
    ) {
        suspend fun resolveUserAndStart() {
            try {
                val workflow = getWorkflow()

                val canDelete = user == null ||
                    !doFilterForUser(user) ||
                    checkUserPermissionsWithResource(
                        user,
                        workflow.user,
                        actionListener,
                        "workflow",
                        workflowId
                    )

                if (canDelete) {
                    val deleteResponse = deleteWorkflow(workflow)
                    // TODO - uncomment once the workflow metadata is added
                    // deleteMetadata(workflow)
                    if (deleteUnderlyingMonitors == true) {
                        val underlyingMonitorIds = (workflow.inputs[0] as CompositeInput).getMonitorIds()
                        val monitorIdsToBeDeleted = monitorsAreNotInDifferentWorkflows(workflowId, underlyingMonitorIds)

                        // Delete the monitor ids
                        if (!monitorIdsToBeDeleted.isNullOrEmpty()) {
                            deleteMonitors(monitorIdsToBeDeleted, RefreshPolicy.IMMEDIATE)
                        }
                    }
                    actionListener.onResponse(DeleteWorkflowResponse(deleteResponse.id, deleteResponse.version))
                } else {
                    actionListener.onFailure(
                        AlertingException(
                            "Not allowed to delete this workflow!",
                            RestStatus.FORBIDDEN,
                            IllegalStateException()
                        )
                    )
                }
            } catch (t: Exception) {
                if (t is IndexNotFoundException) {
                    actionListener.onFailure(
                        OpenSearchStatusException(
                            "Workflow not found.",
                            RestStatus.NOT_FOUND
                        )
                    )
                } else {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        }

        private suspend fun deleteMonitors(monitorIds: List<String>, refreshPolicy: RefreshPolicy) {
            if (monitorIds.isNullOrEmpty())
                return

            for (monitorId in monitorIds) {
                val deleteRequest = DeleteMonitorRequest(monitorId, refreshPolicy)
                val searchResponse: DeleteMonitorResponse = client.suspendUntil {
                    AlertingPluginInterface.deleteMonitor(this as NodeClient, deleteRequest, it)
                }
            }
        }

        private suspend fun monitorsAreNotInDifferentWorkflows(workflowIdToBeDeleted: String, monitorIds: List<String>): List<String> {
            val queryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("_id", workflowIdToBeDeleted)).filter(
                QueryBuilders.nestedQuery(
                    Workflow.WORKFLOW_DELEGATE_PATH,
                    QueryBuilders.boolQuery().must(
                        QueryBuilders.termsQuery(
                            Workflow.WORKFLOW_MONITOR_PATH,
                            monitorIds
                        )
                    ),
                    ScoreMode.None
                )
            )

            val searchRequest = SearchRequest()
                .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .source(SearchSourceBuilder().query(queryBuilder).fetchSource(true))

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }

            val workflows = searchResponse.hits.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef, XContentType.JSON
                ).also { it.nextToken() }
                lateinit var workflow: Workflow
                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                    xcp.nextToken()
                    when (xcp.currentName()) {
                        "workflow" -> workflow = Workflow.parse(xcp)
                    }
                }
                workflow.copy(id = hit.id, version = hit.version)
            }
            val workflowMonitors = workflows.filter { it.id != workflowIdToBeDeleted }.flatMap { (it.inputs[0] as CompositeInput).getMonitorIds() }.distinct()

            return monitorIds.minus(workflowMonitors.toSet())
        }

        fun parse(xcp: XContentParser, id: String = Workflow.NO_ID, version: Long = Workflow.NO_VERSION): Workflow {
            var name: String? = null
            var workflowType: String = Workflow.WorkflowType.COMPOSITE.toString()
            var user: User? = null
            var schedule: Schedule? = null
            var lastUpdateTime: Instant? = null
            var enabledTime: Instant? = null
            var enabled = true
            var schemaVersion = IndexUtils.NO_SCHEMA_VERSION
            val inputs: MutableList<WorkflowInput> = mutableListOf()
            var owner = "alerting"

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Workflow.SCHEMA_VERSION_FIELD -> schemaVersion = xcp.intValue()
                    Workflow.NAME_FIELD -> name = xcp.text()
                    Workflow.WORKFLOW_TYPE_FIELD -> {
                        workflowType = xcp.text()
                        val allowedTypes = Workflow.WorkflowType.values().map { it.value }
                        if (!allowedTypes.contains(workflowType)) {
                            throw IllegalStateException("Workflow type should be one of $allowedTypes")
                        }
                    }
                    Workflow.USER_FIELD -> {
                        user = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else User.parse(xcp)
                    }
                    Workflow.ENABLED_FIELD -> enabled = xcp.booleanValue()
                    Workflow.SCHEDULE_FIELD -> schedule = Schedule.parse(xcp)
                    Workflow.INPUTS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(
                            XContentParser.Token.START_ARRAY,
                            xcp.currentToken(),
                            xcp
                        )
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            val input = WorkflowInput.parse(xcp)
                            inputs.add(input)
                        }
                    }
                    Workflow.ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    Workflow.LAST_UPDATE_TIME_FIELD -> lastUpdateTime = xcp.instant()
                    Workflow.OWNER_FIELD -> {
                        owner = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) owner else xcp.text()
                    }
                    else -> {
                        xcp.skipChildren()
                    }
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }
            return Workflow(
                id,
                version,
                requireNotNull(name) { "Workflow name is null" },
                enabled,
                requireNotNull(schedule) { "Workflow schedule is null" },
                lastUpdateTime ?: Instant.now(),
                enabledTime,
                Workflow.WorkflowType.valueOf(workflowType.uppercase(Locale.ROOT)),
                user,
                schemaVersion,
                inputs.toList(),
                owner
            )
        }

        private suspend fun getWorkflow(): Workflow {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, workflowId)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (getResponse.isExists == false) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Workflow not found.", RestStatus.NOT_FOUND)
                    )
                )
            }
            val xcp = XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            )
            return ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Workflow
        }

        private suspend fun deleteWorkflow(workflow: Workflow): DeleteResponse {
            log.debug("Deleting the workflow with id ${deleteRequest.id()}")
            return client.suspendUntil { delete(deleteRequest, it) }
        }

        private suspend fun deleteMetadata(workflow: Workflow) {
            val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, "${workflow.id}-metadata")
            val deleteResponse: DeleteResponse = client.suspendUntil { delete(deleteRequest, it) }
        }
    }
}
