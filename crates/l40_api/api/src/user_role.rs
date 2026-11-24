//! The `users.role` column type, owned by the accounts store, and the rules
//! for which roles a role may grant or act on.

pub use pplx_space_accounts_store::{UserRole, UserRoleParseError};

/// Which roles a user's role may assign or act on.
pub trait UserRolePolicy {
    fn can_assign_role(&self, role: UserRole) -> bool;
    fn can_mutate_role_holder(&self, role: UserRole) -> bool;
}

impl UserRolePolicy for UserRole {
    fn can_assign_role(&self, role: UserRole) -> bool {
        matches!(
            (*self, role),
            (UserRole::OrganizationAdmin, UserRole::OrganizationAdmin)
                | (UserRole::OrganizationAdmin, UserRole::Member)
        )
    }

    fn can_mutate_role_holder(&self, role: UserRole) -> bool {
        matches!(
            (*self, role),
            (UserRole::OrganizationAdmin, UserRole::OrganizationAdmin)
                | (UserRole::OrganizationAdmin, UserRole::Member)
        )
    }
}
