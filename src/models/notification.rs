use uuid::Uuid;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Notification {
    pub userid: Uuid,
    pub sourceid: Uuid,
	pub typ: String
}

impl Notification {
    pub fn new_empty() -> Self {
        Self {
            userid: Uuid::nil(),
            sourceid: Uuid::nil(), 
            typ: String::new()
        }
    }

    pub fn new(userid: Uuid, sourceid: Uuid, typ: String) -> Self {
        Self {
            userid,
            sourceid,
            typ
        }
    }
}