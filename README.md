# tap-listrak

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Listrak SOAP
  API](https://webservices.listrak.com/SoapWSDL.aspx)
- Extracts the following resources:
  - [Lists](https://webservices.listrak.com/v31/IntegrationService.asmx?op=GetContactListCollection)
  - [Messages](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportListMessageActivity)
  - [Message Clicks](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportRangeMessageContactClick)
  - [Message Opens](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportRangeMessageContactOpen)
  - [Message Reads](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportRangeMessageContactRead)
  - [Message Sends](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportMessageContactSent)
  - [Message Bounces](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportRangeMessageContactBounces)
  - [Message Unsubscribes](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportRangeMessageContactRemoval)
  - [Subscribed Contacts](https://webservices.listrak.com/v31/IntegrationService.asmx?op=ReportRangeSubscribedContacts)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Quick Start

1. Install

    pip install tap-listrak

2. Create the config file

   You must create a JSON configuration file that looks like this:

   ```json
   {
     "start_date": "2010-01-01",
     "username": "your-listrak-username",
     "password": "your-listrak-password"
   }
   ```

   The `start_date` is the date at which the tap will begin pulling data. The
   Listrak API uses a form of HTTP Basic Authentication, meaning the username
   and password you use to login to Listrak must be provided.

4. Run the Tap in Discovery Mode

    tap-listrak -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

5. Run the Tap in Sync Mode

    tap-listrak -c config.json -p catalog-file.json

## Stream Dependencies

You must select the `lists` stream in order for any others to sync. The
`messages` and `subscribed_contacts` contacts stream depend directly on the
data fetched by the `lists` stream.

Additionally, the `messages` stream must be selected for any of the `message_*`
streams to function, as those streams depend on the data fetched by `messages`.

`lists` and `messages` are selected by default.

## Notes on Bookmarking

Due to the dependency structure of the Listrak API, we cannot avoid pulling all
`lists` and `messages` during every run of the tap. An example of why: a
message that was created in 2015 could theoretically have been opened by a
contact in 2017.

This means if you have a very large number of messages, they will be synced
every run. You could avoid this by running an initial sync using an older
`start_date` in your config and then update `start_date` to something more
recent. This will result in only messages created since `start_date` to be
synced, but it also means any `message_sends`, `message_opens`, etc. for
messages created before this `start_date` will not be synced.

---

Copyright &copy; 2017 Fishtown Analytics
