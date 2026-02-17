# How to Obtain O365 / Microsoft Graph API Credentials

This guide explains how to set up an App Registration in Microsoft Entra ID (formerly Azure Active Directory) to allow the Open Data Platform to read emails via the Microsoft Graph API.

## Prerequisites
- Access to the [Azure Portal](https://portal.azure.com)
- **Global Administrator** or **Application Administrator** role in your Microsoft 365 Tenant (required to grant permissions).

## Step 1: Create an App Registration
1. Navigate to **Microsoft Entra ID** (Service) in the Azure Portal.
2. In the left menu, select **App registrations**.
3. Click **+ New registration**.
4. Fill in the details:
   - **Name**: `Open Data Platform ETL` (or similar).
   - **Supported account types**: Select **Accounts in this organizational directory only (Single tenant)**.
   - **Redirect URI**: Leave blank (not needed for background services).
5. Click **Register**.

## Step 2: Get IDs (Client ID & Tenant ID)
Once created, you will be on the **Overview** page. Copy the following values to your `.env` file:
- **Application (client) ID** -> `O365_CLIENT_ID`
- **Directory (tenant) ID** -> `O365_TENANT_ID`

## Step 3: Create a Client Secret
1. In the left menu, select **Certificates & secrets**.
2. Under the **Client secrets** tab, click **+ New client secret**.
3. **Description**: `Data Platform Key` (or similar).
4. **Expires**: Select `Recommended: 180 days` (or your preference).
5. Click **Add**.
6. **IMPORTANT**: Copy the **Value** (not the Secret ID) immediately. This is the only time it will be visible.
   - **Value** -> `O365_CLIENT_SECRET` in your `.env` file.

## Step 4: Configure API Permissions
The application needs permission to read emails without a signed-in user (background service).
1. In the left menu, select **API permissions**.
2. Click **+ Add a permission**.
3. Select **Microsoft Graph**.
4. Select **Application permissions** (NOT Delegated permissions).
5. Search for `Mail` and check:
   - `Mail.Read` (Read mail in all mailboxes)
   - Alternatively, use `Mail.ReadBasic` if you don't need body content, but for this pipeline we likely need body.
6. Click **Add permissions**.

## Step 5: Grant Admin Consent
After adding permissions, they will show as "Not granted".
1. Click the **Grant admin consent for [Your Organization]** button next to the "Add a permission" button.
2. Click **Yes** to confirm.
   - The status column should change to a green checkmark "Granted for ...".

## Step 6: Restrict Access (Optional but Recommended)
By default, the `Mail.Read` application permission grants access to **ALL** mailboxes in the tenant. To restrict access to only specific mailboxes (e.g., `info@`, `support@`):

1. **Create a Mail-Enabled Security Group** in M365 Admin Center containing the target mailboxes.
2. **Use PowerShell** to create an Application Access Policy:
   ```powershell
   # Connect to Exchange Online
   Connect-ExchangeOnline

   # Create Policy
   New-ApplicationAccessPolicy -AppId "<Your-App-Client-ID>" -PolicyScopeGroupId "<Security-Group-Email-Address>" -AccessRight RestrictAccess -Description "Restrict Data Platform to specific mailboxes"
   ```

## Final Config
Ensure your `.env` file has:
```env
O365_CLIENT_ID=<Application (client) ID>
O365_CLIENT_SECRET=<Client Secret Value>
O365_TENANT_ID=<Directory (tenant) ID>
```
