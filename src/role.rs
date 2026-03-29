use std::fmt;

use anyhow::{Result, bail};

// Роль узла определяется по направлению данных.
// Это важно: sender/receiver не зависят от того, кто был TCP-клиентом.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// Числовые значения входят в wire protocol, поэтому их нельзя менять без
// отдельной миграции версии протокола.
#[repr(u8)]
pub enum Role {
    Sender = 1,
    Receiver = 2,
}

impl Role {
    // Преобразуем байт из сетевого пакета обратно в enum.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Sender),
            2 => Ok(Self::Receiver),
            _ => bail!("unknown role value: {value}"),
        }
    }

    // Это значение уходит в минимальный handshake-пакет.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    // Строковое имя используем в логах и ошибках.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sender => "sender",
            Self::Receiver => "receiver",
        }
    }
}

impl fmt::Display for Role {
    // Благодаря Display роль можно удобно печатать через `{}`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::Role;

    #[test]
    fn wire_values_and_display_are_stable() {
        assert_eq!(Role::Sender.as_u8(), 1);
        assert_eq!(Role::Receiver.as_u8(), 2);

        assert_eq!(Role::Sender.as_str(), "sender");
        assert_eq!(Role::Receiver.as_str(), "receiver");

        assert_eq!(Role::Sender.to_string(), "sender");
        assert_eq!(Role::Receiver.to_string(), "receiver");

        assert_eq!(Role::from_u8(1).unwrap(), Role::Sender);
        assert_eq!(Role::from_u8(2).unwrap(), Role::Receiver);
    }

    #[test]
    fn rejects_unknown_wire_value() {
        let error = Role::from_u8(7).unwrap_err();

        assert!(error.to_string().contains("unknown role value: 7"));
    }
}
