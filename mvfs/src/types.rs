#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockKind {
    None,
    Shared,
    Reserved,
    Pending,
    Exclusive,
}

impl LockKind {
    pub fn level(self) -> u32 {
        match self {
            LockKind::None => 0,
            LockKind::Shared => 1,
            LockKind::Reserved => 2,
            LockKind::Pending => 3,
            LockKind::Exclusive => 4,
        }
    }
}
