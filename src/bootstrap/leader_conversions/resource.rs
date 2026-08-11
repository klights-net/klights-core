#[cfg(test)]
use klights_leader_api::ResourceListRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct ListRequest {
    pub(crate) api_version: String,
    pub(crate) kind: String,
    pub(crate) namespace: Option<String>,
    pub(crate) label_selector: Option<String>,
    pub(crate) field_selector: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) continue_token: Option<String>,
}

#[cfg(test)]
pub(crate) fn legacy_list_request(request: &ResourceListRequest) -> ListRequest {
    ListRequest {
        api_version: request.api_version().to_string(),
        kind: request.kind().to_string(),
        namespace: request.namespace().map(str::to_owned),
        label_selector: request.label_selector().map(str::to_owned),
        field_selector: request.field_selector().map(str::to_owned),
        limit: request.limit(),
        continue_token: request.continue_token().map(str::to_owned),
    }
}
