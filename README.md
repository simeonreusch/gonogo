# gonogo
Decide if GW alerts are worthy of ZTF follow-up. The decision for each Kafka message from the LVK GCN stream is posted on Slack.

## Decision Matrix
Read from left to right, columns are `AND`

Parameter | Go-Deep | Go-Wide | Decide | No-Go
-------- | -------- | -------- | ---- | ------
Strategy   | Overwrite schedule (ToO), 300s   | Adjust schedule, 30s | If it's worthy | Ignore |
Expected rate   | 3 nights/month   | 5 nights/month | 
FAR | < 1/century | < 1/decade | < 1/year | >= 1/year | 
Has neutron star | > 0.9 | > 0.9 | > 0.1 | 
Has remnant | `true` | `true` | 
p(BNS + NSBH) | > 0.5 | > 0.5 | > 0.1 |

## How to run
You need Kafka credentials, obtain them from the [GCN page](https://gcn.nasa.gov/quickstart), and a Slackbot token from the [Slack API](api.slack.com).