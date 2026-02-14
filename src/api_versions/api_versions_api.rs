use core::error;

use crate::{
    api_keys::{API_VERSIONS, DESCRIBE_TOPIC_PARTITIONS, FETCH, PRODUCE},
    api_response::{self, ApiResponse},
    api_versions::{
        self,
        api_versions_response_v4::{ApiKey, ApiVersionsResponseV4},
    },
    describe_topic_partitions, error_codes, fetch,
    headers::{request_header_v2::RequestHeaderV2, response_header_v0::ResponseHeaderV0},
    produce,
    tagged_fields_section::TaggedFieldsSection,
    types::compact_array::CompactArray,
};

const SUPPORTED_API_VERSIONS: [i16; 5] = [0, 1, 2, 3, 4];

pub struct ApiVersionsApi;

impl ApiVersionsApi {
    pub fn respond(
        request_header: RequestHeaderV2,
    ) -> ApiResponse<ResponseHeaderV0, ApiVersionsResponseV4> {
        let error_code = Self::check_supported_version(request_header.request_api_version);
        let api_keys = Self::api_keys();
        let throttle_time_ms = 0;
        let response = ApiVersionsResponseV4::new(
            error_code,
            api_keys,
            throttle_time_ms,
            TaggedFieldsSection::empty(),
        );
        api_response::v0(request_header.correlation_id, response)
    }

    fn check_supported_version(version: i16) -> i16 {
        if SUPPORTED_API_VERSIONS.contains(&version) {
            error_codes::NONE
        } else {
            error_codes::UNSUPPORTED_VERSION
        }
    }

    fn api_keys() -> CompactArray<ApiKey> {
        [
            ApiKey::new(
                API_VERSIONS,
                api_versions::MIN_VERSION,
                api_versions::MAX_VERSION,
                TaggedFieldsSection::empty(),
            ),
            ApiKey::new(
                FETCH,
                fetch::MIN_VERSION,
                fetch::MAX_VERSION,
                TaggedFieldsSection::empty(),
            ),
            ApiKey::new(
                DESCRIBE_TOPIC_PARTITIONS,
                describe_topic_partitions::MIN_VERSION,
                describe_topic_partitions::MAX_VERSION,
                TaggedFieldsSection::empty(),
            ),
            ApiKey::new(
                PRODUCE,
                produce::MIN_VERSION,
                produce::MAX_VERSION,
                TaggedFieldsSection::empty(),
            ),
        ]
        .into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use parameterized::parameterized;

    #[parameterized(
        version = {
            0, 1, 2, 3, 4
        }
    )]
    fn checks_supported_version(version: i16) {
        assert_eq!(
            error_codes::NONE,
            ApiVersionsApi::check_supported_version(version)
        );
    }

    #[test]
    fn checks_unsupported_version() {
        assert_eq!(
            error_codes::UNSUPPORTED_VERSION,
            ApiVersionsApi::check_supported_version(6)
        );
    }
}
