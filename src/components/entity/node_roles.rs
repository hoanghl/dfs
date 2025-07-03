use std::str::FromStr;

// ================================================
// Definition
// ================================================

#[rustfmt::skip]
#[repr(u8)]
#[derive(Clone, Copy)]
pub enum Role {
    Default = 0,
    Master  = 1,
    Data    = 2,
    DNS     = 3,
    Client     = 4,
}
// ================================================
// Implementations
// ================================================
impl From<u8> for Role {
    fn from(value: u8) -> Self {
        match value {
            1 => Role::Master,
            2 => Role::Data,
            3 => Role::DNS,
            4 => Role::Client,
            _ => panic!("Error as parsing to enum Role: value = {}", value),
        }
    }
}

impl From<&Role> for u8 {
    fn from(value: &Role) -> Self {
        match value {
            Role::Master => 1,
            Role::Data => 2,
            Role::DNS => 3,
            Role::Client => 4,
            _ => panic!("Error as parsing from enum Role"),
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Role::Default => "Default",
            Role::Master => "Master",
            Role::Data => "Data",
            Role::DNS => "DNS",
            Role::Client => "Client",
        };
        write!(f, "{}", s)
    }
}

impl std::fmt::Debug for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Role::Default => "Default",
            Role::Master => "Master",
            Role::Data => "Data",
            Role::DNS => "DNS",
            Role::Client => "Client",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for Role {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "dns" => {
                return Ok(Self::DNS);
            }
            "master" => {
                return Ok(Self::Master);
            }
            "data" => {
                return Ok(Self::Data);
            }
            "client" => {
                return Ok(Self::Client);
            }
            _ => Err(format!("Cannot parse given string to Role. Got: {}", s)),
        }
    }
}
