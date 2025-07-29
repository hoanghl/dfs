pub enum DBManagerCreationErrorCode {
    Default,
}

pub struct DBManagerCreationError {
    pub error_code: DBManagerCreationErrorCode,
}
