# Cleanz — SF Plugin

Automatically fixes Permission Set and Profile deployment errors so you don't have to do it manually.

---

## What does it do?

When you deploy Permission Sets or Profiles to a Salesforce org, you often get errors like:

- `No such column 'MyField__c' on entity 'MyObject__c'`
- `Unknown user permission: SomePermission`
- `Invalid record type: MyRecordType`

Normally you would open each file, find the bad reference, delete it, and redeploy — one by one. For a promotion with 10+ permission sets, this takes hours.

**Cleanz does all of that automatically in minutes.**

---

## How it works

1. You give it a list of components (Copado JSON or package.xml) and a target org
2. It deploys only the Permission Sets, Muting Permission Sets and Profiles
3. When errors come back, it reads them, finds the bad references in the XML files, removes them, and commits the fix
4. It redeploys and repeats until everything passes or there is nothing left to fix
5. At the end it sweeps the entire repo for the same bad references in other files too

---

## Install

```bash
sf plugins install @naveengit9/plugin-cleanz
```

To update to the latest version:

```bash
sf plugins update
```

---

## Run

```bash
sf cleanz run
```

It will ask you two questions:

1. Path to your Copado Promotion JSON or package.xml
2. Target org alias or username

Or pass them directly:

```bash
sf cleanz run --json-path C:\Users\YourName\Desktop\promotion.json --target-org RBKQA
sf cleanz run --json-path C:\package.xml --target-org RBKQA
```

### Flags

| Flag           | Short | Description                                                           |
| -------------- | ----- | --------------------------------------------------------------------- |
| `--json-path`  | `-j`  | Full path to your Copado Promotion JSON or package.xml                |
| `--target-org` | `-t`  | Target org username or alias                                          |
| `--verbose`    | `-v`  | Show all error details for debugging                                  |
| `--dry-run`    | `-d`  | Fix files and show what would be committed but do not actually commit |

---

## Input formats

### Copado Promotion JSON

Used when working with Copado deployments. Cleanz reads the operation type (`Add` vs `Retrieve`) to decide what to protect.

- **Add** — component is being deployed in this package. If a Permission Set references it, Cleanz will NOT remove it even if it does not exist in the target org yet (it will be there after the real deploy).
- **Retrieve** — component already exists in prod and was just retrieved for permissions. If it does not exist in the target org, Cleanz will safely remove the reference.

```json
[
  { "u": "US-001", "t": "PermissionSet", "n": "My_PermSet", "a": "Add" },
  { "u": "US-001", "t": "CustomField", "n": "MyObject__c.MyField__c", "a": "Add" },
  { "u": "US-001", "t": "CustomField", "n": "MyObject__c.OtherField__c", "a": "Retrieve" }
]
```

### package.xml

Used when working outside Copado — GitHub Actions, Azure DevOps, Jenkins, or manual SFDX deployments. Since package.xml has no operation concept, all components are treated as `Add` (fully protected).

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
    <types>
        <members>MyObject__c.MyField__c</members>
        <name>CustomField</name>
    </types>
    <types>
        <members>My_PermSet</members>
        <name>PermissionSet</name>
    </types>
    <version>66.0</version>
</Package>
```

---

## Important: One branch per org (for package.xml users)

This is the most important thing to understand if you are using package.xml.

**Cleanz modifies files in your current git branch and commits the fixes.** If the same branch is used to deploy to multiple orgs (QA, UAT, Prod), a reference removed for QA will also be missing when you deploy to UAT or Prod — even if the field exists there.

### Safe setup — one branch per environment (recommended)

```
feature/my-story  →  merge →  qa-branch    →  deploy to QA    (Cleanz runs here)
                              uat-branch   →  deploy to UAT   (Cleanz runs here if needed)
                              prod-branch  →  deploy to Prod  (Cleanz runs here if needed)
```

Each branch gets cleaned for its own target org. Fixes in the QA branch never affect the UAT or Prod branch.

**This is exactly how Copado works** — one promotion branch per target org — which is why Cleanz is a perfect fit for Copado.

### Risky setup — single branch to all envs (avoid)

```
main-branch  →  deploy to QA   (Cleanz removes field reference)
             →  deploy to UAT  (reference is now missing even though field exists in UAT)
             →  deploy to Prod (same problem)
```

If your pipeline uses one branch for all environments, do not use Cleanz on the shared branch. Either run it only on environment-specific branches, or use `--dry-run` to preview what it would do without committing.

---

## Git history after a run

Cleanz creates clean, readable commits:

```
1. Dedup: remove duplicate XML blocks from X file(s)
2. Remove managed package refs (N ref(s) in X file(s)): ...
3. Auto-fix: remove N missing ref(s) across X file(s)
4. Repo-wide sweep: remove N ref(s) from Y file(s) outside promotion batch
```

Commit 1 — deduplication (always separate)
Commit 2 — managed/namespace package ref removals (only if namespace errors occurred)
Commit 3 — all missing ref fixes squashed into one clean commit
Commit 4 — same bad refs removed from all other files in the repo (only if found)

---

## Commands

- [`sf cleanz run`](#sf-cleanz-run)

## `sf cleanz run`

Automated Permission Set and Profile deploy and fix.

```
USAGE
  $ sf cleanz run [--json] [--flags-dir <value>] [-j <value>] [-t <value>] [-v] [-d]

FLAGS
  -d, --dry-run             Modify files to remove bad refs but skip all git commits (preview mode).
  -j, --json-path=<value>   Full path to your Copado Promotion JSON or package.xml file.
  -t, --target-org=<value>  Target org username or alias.
  -v, --verbose             Print all individual deployment error details (useful for debugging).

GLOBAL FLAGS
  --flags-dir=<value>  Import flag values from a directory.
  --json               Format output as json.

EXAMPLES
  $ sf cleanz run

  $ sf cleanz run --json-path C:\Users\YourName\Desktop\promotion.json --target-org RBKQA

  $ sf cleanz run --json-path C:\project\package.xml --target-org RBKQA --dry-run
```

---

## Issues

Report issues at https://github.com/NaveenGIT9/Cleanz-Plugin/issues
