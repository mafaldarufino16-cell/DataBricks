# Adding Collaborators to the Repository

This guide provides detailed instructions for adding user access to this repository.

## Table of Contents
- [GitHub Repository Access](#github-repository-access)
- [Permission Levels](#permission-levels)
- [Databricks Workspace Access](#databricks-workspace-access)
- [Best Practices](#best-practices)

## GitHub Repository Access

### Prerequisites
- You must be the repository owner or have admin access
- The user must have a GitHub account

### Step-by-Step Instructions

#### Option 1: Via GitHub Web Interface

1. **Navigate to Repository Settings**
   - Go to https://github.com/FranciscoBelo/Databricks
   - Click on the "Settings" tab (⚙️ icon)

2. **Access Collaborators Section**
   - In the left sidebar, click "Collaborators and teams"
   - You may be prompted to enter your password for security

3. **Add New Collaborator**
   - Click the green "Add people" button
   - In the search box, enter:
     - GitHub username (e.g., `username`)
     - Email address associated with their GitHub account
   - Select the user from the dropdown

4. **Set Permission Level**
   - Choose one of the following roles:
     - **Read**: View and clone only
     - **Write**: Read + push to repository
     - **Admin**: Full access to repository including settings
   - Note: GitHub also offers **Triage** and **Maintain** roles for organization repositories with more granular permissions

5. **Send Invitation**
   - Click "Add [username] to this repository"
   - The user will receive an email invitation
   - They must accept the invitation to gain access

#### Option 2: Via GitHub CLI

If you have GitHub CLI installed:

```bash
# Add a collaborator with write access (push)
gh api repos/FranciscoBelo/Databricks/collaborators/USERNAME -X PUT -f permission=push

# Add a collaborator with read access (pull)
gh api repos/FranciscoBelo/Databricks/collaborators/USERNAME -X PUT -f permission=pull

# Add a collaborator with admin access
gh api repos/FranciscoBelo/Databricks/collaborators/USERNAME -X PUT -f permission=admin
```

Replace `USERNAME` with the actual GitHub username.

## Permission Levels

### Read Access
**Best for**: External contributors, viewers
- View code
- Clone repository
- Open issues
- Comment on issues and pull requests

### Write Access
**Best for**: Active contributors, team members
- All Read permissions
- Push to repository
- Merge pull requests
- Edit wiki

### Admin Access
**Best for**: Repository maintainers, project leads
- All Write permissions
- Manage repository settings
- Add/remove collaborators
- Delete repository

## Databricks Workspace Access

### Adding Users to Databricks Workspace

1. **Access Databricks Admin Console**
   - Log in to your Databricks workspace
   - Click on your user icon (top right)
   - Select "Admin Settings"

2. **Navigate to User Management**
   - Click on "Users" in the left navigation
   - Click "Add User" button

3. **Invite User**
   - Enter the user's email address
   - Optionally assign them to groups
   - Click "Send Invite"

### Granting Database Permissions

Once users have Databricks access, grant them permissions on the catalog and schema used by these scripts:

```sql
-- Replace user@example.com with the actual user email

-- Grant catalog access
GRANT USAGE ON CATALOG databrickstheloop TO `user@example.com`;

-- Grant schema access  
GRANT USAGE ON SCHEMA databrickstheloop.databricksfundamentals TO `user@example.com`;

-- Grant ability to create tables (if needed)
GRANT CREATE ON SCHEMA databrickstheloop.databricksfundamentals TO `user@example.com`;

-- Grant read access to all tables in schema
GRANT SELECT ON SCHEMA databrickstheloop.databricksfundamentals TO `user@example.com`;

-- Grant write access to all tables (if needed)
GRANT MODIFY ON SCHEMA databrickstheloop.databricksfundamentals TO `user@example.com`;

-- Grant access to specific tables only
GRANT SELECT ON TABLE databrickstheloop.databricksfundamentals.dim_date TO `user@example.com`;
GRANT SELECT ON TABLE databrickstheloop.databricksfundamentals.dim_product TO `user@example.com`;
GRANT SELECT ON TABLE databrickstheloop.databricksfundamentals.dim_customer TO `user@example.com`;
GRANT SELECT ON TABLE databrickstheloop.databricksfundamentals.dim_store TO `user@example.com`;
GRANT SELECT ON TABLE databrickstheloop.databricksfundamentals.fact_sales TO `user@example.com`;
```

### Creating Groups (Recommended)

For easier management, create groups and assign permissions to groups:

```sql
-- Create a group
CREATE GROUP IF NOT EXISTS databricks_users;

-- Add user to group (done via Databricks UI or API)

-- Grant permissions to group
GRANT USAGE ON CATALOG databrickstheloop TO databricks_users;
GRANT USAGE ON SCHEMA databrickstheloop.databricksfundamentals TO databricks_users;
GRANT SELECT ON SCHEMA databrickstheloop.databricksfundamentals TO databricks_users;
```

## Best Practices

### Security Recommendations

1. **Principle of Least Privilege**
   - Grant the minimum permissions necessary
   - Start with Read access and upgrade as needed

2. **Regular Access Reviews**
   - Periodically review who has access
   - Remove access for users who no longer need it

3. **Use Teams for Organizations**
   - If this becomes an organization repository, use Teams instead of individual collaborators
   - Easier to manage permissions at scale

4. **Enable Two-Factor Authentication**
   - Require 2FA for all collaborators with Write or Admin access

### Databricks Recommendations

1. **Use Groups Instead of Individual Users**
   - Easier to manage permissions
   - More scalable as team grows

2. **Separate Development and Production**
   - Use different catalogs/schemas for dev and prod
   - Restrict production write access

3. **Monitor Usage**
   - Enable audit logging
   - Review access patterns regularly

4. **Document Permissions**
   - Keep track of who has access and why
   - Document any special permissions granted

## Troubleshooting

### User Can't Accept Invitation
- Verify the email address is correct
- Check spam folder
- Resend invitation from Settings > Collaborators

### User Can't Push to Repository
- Verify they have Write or Admin access
- Check if branch protection rules are enabled
- Ensure they've accepted the invitation

### Databricks Permission Errors
- Verify user is added to workspace
- Check catalog and schema permissions
- Ensure Unity Catalog is properly configured
- Review error message for specific missing permission

## Additional Resources

- [GitHub Docs: Inviting collaborators](https://docs.github.com/en/account-and-profile/setting-up-and-managing-your-personal-account-on-github/managing-access-to-your-personal-repositories/inviting-collaborators-to-a-personal-repository)
- [Databricks: Manage users and groups](https://docs.databricks.com/administration-guide/users-groups/index.html)
- [Databricks: Unity Catalog privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/index.html)
