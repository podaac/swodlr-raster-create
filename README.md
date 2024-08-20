# swodlr-raster-create

`swodlr-raster-create` is an asynchronous SWODLR microservice designed to
interface with a HySDS backend to process user requests for generating raster
products. This service on its own does not generate raster products or perform
job tracking; these responsibilities are instead handed off to the HySDS
backend.

## Design

`swodlr-raster-create` was designed as part of the larger SWODLR system and as
such isn't intended for solo operation. `swodlr-raster-create` is a series of
AWS Lambdas orchestrated with an AWS Step Function. This Step Function is
triggered off a SNS/SQS queue which receives JSON messages that follow the
[SWODLR input schema](https://github.com/podaac/swodlr-schemas/blob/main/input.json)
which is sent from the `swodlr-api` microservice which fields user requests and
performs caching prior to this layer. `swodlr-raster-create` does not perform
user authentication and as such relies on any microservice which invokes it
to perform this authentication. `swodlr-raster-create` has no awareness of
a "user" and only deals with product IDs internal to SWODLR.

### Workflow

When an SNS message is received into the `swodlr-raster-create` input topic,
it is automatically transferred to an SQS queue where it will await processing
by the `bootstrap` lambda. The bootstrap lambda simply takes the original
message from the queue and repackages it as JSON input to the Step Function
invocation.

`swodlr-raster-create` then runs its `preflight` process. This process performs
a reconciliation between the SDS's internal granule database and CMR for the
granules that are required for raster product generation. These granules include
the PIXC, PIXCVec, and Orbit input granules. If a granule is found in CMR that
isn't in the SDS database, the granule will be ingested into the SDS. If a
granule is found in the SDS that isn't in the SDS, the granule will be deleted
from the SDS database. `preflight` ensures that data between CMR and the SDS
remain in sync and that any granules that weren't ingested as part of
`swodlr-ingest-to-sds` due to any technical issues (networking, downtime, etc)
are present before a granule generation is kicked off. Preflight also performs
a translation from the `input` schema to the
[jobset schema](https://github.com/podaac/swodlr-schemas/blob/main/jobset.json).
This schema enables `swodlr-raster-create` to track jobs across multiple
lambdas, maintain the original input data, and trigger Step Function polling
logic.

Once preflight is completed, `swodlr-raster-create` then kicks off the `submit_
evaluate` process. For this process, the SDS takes one PIXCVec tile within
the scene that is being requested as an input. The SDS then generates a state
configuration for the scene that is to be generated that contains all the
tiles/granules within the scene and an Orbit granule to use. The SDS will use
this state config later on in the raster generation process.

Before the next stage starts, `swodlr-raster-create` notifies an SNS topic
on the status of the state configuration generation and that generation will
begin if the state configuration generation was successful. This SNS topic is
currently listened to by `swodlr-async-update` and `swodlr-user-notify` which
independently determine what actions to take based on the current `jobset` they
receive from this service. For more information on how these services process
jobsets, please view the documentation in each respective repo.

After `submit_evaluate`, the `submit_raster` process kicks off which is where
raster generation is actually initiated on the SDS. This is also where
additional configuration values for the SDS such as raster resolution/grid
type are handed off to customize the raster being generated. The state config
generated in a previous step is retrieved and generation is kicked off using
this configuration as the input.

Once granule generation is finished, the `publish_data` Lambda takes the
completed jobset and attempts to retrieve the finalized raster granules from the
SDS and copy them from the SDS's bucket to a PO.DAAC managed bucket. Once this
process is complete, `publish_data` adds the S3 URL to the `job` within the
`jobset`. If either data retrieval or S3 copying fails for any reason, SWODLR
marks these jobs as a failure. Several attempts will be made to retrieve/copy
the data before the job is marked as a failure.

Finally, `swodlr-raster-create` once again notifies the update SNS topic where
it hands off the finalized jobset to the `swodlr-async-update` and
`swodlr-user-notify` services to perform additional processing such as updating
the `swodlr-api` database or emailing users to notify them of their completed
job.
