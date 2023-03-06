package org.opensearch.alerting

import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.client.RestClient
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.string
import org.opensearch.rest.RestStatus

class WorkflowRestTestCase : AlertingRestTestCase() {
    private fun createWorkflowEntityWithBackendRoles(workflow: Workflow, rbacRoles: List<String>?): HttpEntity {
        if (rbacRoles == null) {
            return workflow.toHttpEntity()
        }
        val temp = workflow.toJsonString()
        val toReplace = temp.lastIndexOf("}")
        val rbacString = rbacRoles.joinToString { "\"$it\"" }
        val jsonString = temp.substring(0, toReplace) + ", \"rbac_roles\": [$rbacString] }"
        return StringEntity(jsonString, ContentType.APPLICATION_JSON)
    }

    protected fun createWorkflowWithClient(
        client: RestClient,
        workflow: Workflow,
        rbacRoles: List<String>? = null,
        refresh: Boolean = true
    ) {
        val response = client.makeRequest(
            "POST", "$WORKFLOW_ALERTING_BASE_URI?refresh=$refresh", emptyMap(),
            createWorkflowEntityWithBackendRoles(workflow, rbacRoles)
        )
        assertEquals("Unable to create a new monitor", RestStatus.CREATED, response.restStatus())

        val monitorJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        assertUserNull(monitorJson as HashMap<String, Any>)
    }

    protected fun createWorkflow(workflow: Workflow, refresh: Boolean = true) {
        createWorkflowWithClient(client(), workflow, emptyList(), refresh)
    }

    protected fun Workflow.toHttpEntity(): HttpEntity {
        return StringEntity(toJsonString(), ContentType.APPLICATION_JSON)
    }

    private fun Workflow.toJsonString(): String {
        val builder = XContentFactory.jsonBuilder()
        return shuffleXContent(toXContent(builder, ToXContent.EMPTY_PARAMS)).string()
    }

}