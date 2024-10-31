# ![datavolo.io](https://docs.datavolo.io/img/logo-without-name.svg) Datavolo Flow Diff for Apache NiFi

This action is brought to you by [Datavolo](https://datavolo.io/), don't hesitate to visit our website and reach out to us!
You can also join our [Slack workspace](https://join.slack.com/t/datavolocommunity/shared_invite/zt-2clo9iv4h-MFeyT8_HPKkJQM8PskkPSA)
if you have questions about this action, about Apache NiFi in general or about how to integrate NiFi in your CI/CD pipelines.

## Usage

When using the GitHub Flow Registry Client in NiFi to version control your flows, add the below file `.github/workflows/flowdiff.yml` to the repository into which flow definitions are versioned.

Whenever a pull request is opened, reopened or when a new commit is pushed to an existing pull request, this workflow will be triggered and will compare the modified flow in the pull request and will
automatically comment the pull request with a human readable description of the changes included in the pull request.

```yaml
name: Datavolo Flow Diff on Pull Requests
on:
  pull_request:
    types: [opened, reopened, synchronize]

jobs:
  execute_flow_diff:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write
    name: Executing Flow Diff
    steps:
      # checking out the code of the pull request (merge commit - if the PR is mergeable)
      - name: Checkout PR code
        uses: actions/checkout@v4
        with:
          path: submitted-changes

      # getting the path of the flow definition that changed (only one expected for now)
      - id: files
        uses: Ana06/get-changed-files@v1.2

      # checking out the code without the change of the PR
      - name: Checkout original code
        uses: actions/checkout@v4
        with:
          fetch-depth: 2
          path: original-code
      - run: cd original-code && git checkout HEAD^

      # Running the diff
      - name: Datavolo Flow Diff
        uses: datavolo-io/datavolo-flow-diff@v0
        id: flowdiff
        with:
          flowA: 'original-code/${{ steps.files.outputs.all }}'
          flowB: 'submitted-changes/${{ steps.files.outputs.all }}'
```

## Example

The GitHub Action will automatically publish a comment on the pull request with a comprehensive description of the changes between the flows of the two branches.
Here is an example of what the comment could look like:

```markdown
### Executing Datavolo Flow Diff for flow: `MyExample`

- The destination of a connection has changed from `UpdateAttribute` to `InvokeHTTP`
- A self-loop connection `[success]` has been added on `UpdateAttribute`
- A connection `[success]` from `My Generate FlowFile Processor` to `UpdateAttribute` has been added
- A Processor has been renamed from `GenerateFlowFile` to `My Generate FlowFile Processor`
- In processor named `GenerateFlowFile`, the Scheduling Strategy changed from `TIMER_DRIVEN` to `CRON_DRIVEN`
- A Parameter Context named `Test Parameter Context` has been added
- The parameter context `Test Parameter Context` with parameters `{addedParam=newValue}` has been added to the process group `TestingFlowDiff`
- A Processor named `UpdateAttribute` has been removed
- The bundle `org.apache.nifi:nifi-standard-nar` has been changed from version `2.0.0-M4` to version `2.0.0`
- In processor `GenerateFlowFile`, the Run Schedule changed from `1 min` to `* * * * * ?`
- A Parameter Context named `Another one to delete` has been added
- A Processor `UpdateAttribute` has been added with the below configuration
  - `Store State` = `Do not store state`
  - `canonical-value-lookup-cache-size` = `100`
```
