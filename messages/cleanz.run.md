# summary

Automated Permission Set and Profile deploy and fix.

# description

Reads a Copado Promotion JSON, dry-runs deploys, and auto-removes missing custom field references from Permission Sets and Profiles.

# examples

- <%= config.bin %> <%= command.id %>
- <%= config.bin %> <%= command.id %> --json-path C:\Users\YourName\Desktop\promotion.json --target-org myOrg

# flags.json-path.summary

Full path to your Copado Promotion JSON file.

# flags.target-org.summary

Target org username or alias.

# flags.verbose.summary

Print all individual deployment error details (useful for debugging).
