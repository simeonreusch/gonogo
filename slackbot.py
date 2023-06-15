#!/usr/bin/env python3
# Author: Simeon Reusch (simeon.reusch@desy.de)
# License: BSD-3-Clause

import argparse
import datetime
import json
import logging
import os
from base64 import b64decode
from io import BytesIO
from pathlib import Path
from typing import Dict

import astropy.units as u
import astropy_healpix as ah  # type: ignore
import numpy as np
from astropy.table import Table  # type: ignore
from gcn_kafka import Consumer  # type: ignore
from slack import WebClient

LVK_GCN_ID = os.environ.get("LVK_GCN_ID")
LVK_GCN_TOKEN = os.environ.get("LVK_GCN_TOKEN")
LVK_GCN_GROUP = os.environ.get("LVK_GCN_GROUP")  # this can be an arbitrary string
SLACK_TOKEN = os.environ.get("SLACK_TOKEN_GONOGO")

PNS_THRESHOLD_DELIBERATE: float = 0.1
PNS_THRESHOLD_GO: float = 0.5
FAR_THRESHOLD_YEAR: float = 3.17e-8
FAR_THRESHOLD_DECADE: float = 3.17e-9
FAR_THRESHOLD_CENTURY: float = 3.17e-10
HAS_NS_THRESHOLD_GO: float = 0.9
HAS_NS_THRESHOLD_DELIBERATE: float = 0.1
HAS_REMNANT_THRESHOLD: float = 0.0

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_notice(record_raw: str) -> dict | None:
    """
    Parse the json of the notice and extract location and distance
    """
    record = json.loads(record_raw)

    event_id = record["superevent_id"]

    if event_id[0] not in ["S"]:
        logger.info(f"{event_id}: Mock signal. Discarding")
        return None

    if record["alert_type"] == "RETRACTION":
        logger.info(f"{event_id}: Retracted")
        return None

    # Respond only to 'CBC' events. Change 'CBC' to 'Burst' to respond to
    # only unmodeled burst events.
    if record["event"]["group"] not in ["CBC", "Burst"]:
        logger.info(
            f"{event_id}: Pipeline neither 'CBC' nor 'Burst' {record['event']['group']}. Discarding"
        )
        return None

    logger.info(f"{event_id}: Received. Evaluating now")

    # Parse sky map
    skymap_str = record.get("event", {}).pop("skymap")
    if skymap_str:
        # Decode, parse skymap, and print most probable sky location
        skymap_bytes = b64decode(skymap_str)
        skymap = Table.read(BytesIO(skymap_bytes))

        level, ipix = ah.uniq_to_level_ipix(
            skymap[np.argmax(skymap["PROBDENSITY"])]["UNIQ"]
        )
        ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level), order="nested")

        dist = skymap.meta.get("DISTMEAN")
        dist_unc = skymap.meta.get("DISTSTD")

        logger.info(f"{event_id}: Has skymap. Continuing")

        return {
            "loc": [ra.deg, dec.deg],
            "dist": [dist, dist_unc],
            "record": record,
            "event_id": event_id,
            "pipeline": record["event"]["group"],
        }

    else:
        logger.info(f"{event_id}: Has no skymap. Discarding")
        return None


def decide(record: dict) -> dict | None:
    """
    Evaluate the alert content, decide if it warrants further scrutiny or if it is an automatic nogo
    """
    status: Dict[str, str | list] = {"status": "undecided"}
    reason: list = []
    details = record.get("record", {})

    if details is not None:
        event_id = details["superevent_id"]
        event = details["event"]
        far = event["far"]
        classification = event["classification"]
        status["url"] = details["urls"].get("gracedb")

        status["id"] = event_id
        status["FAR"] = far

        period = 1 / far * u.s
        period_year = period.to(u.year)
        far_yearly = 1 / period_year.value
        status["far_yearly"] = far_yearly

        pipeline = record["pipeline"]
        status["pipeline"] = pipeline

        if pipeline == "CBC":
            has_ns = event["properties"].get("HasNS")
            has_remnant = event["properties"].get("HasRemnant")
            bns = classification["BNS"]
            combined_ns = classification["BNS"] + classification["NSBH"]

            status["pNS"] = combined_ns
            status["hasRemnant"] = has_remnant

            if (
                combined_ns > PNS_THRESHOLD_GO
                and has_ns > HAS_NS_THRESHOLD_GO
                and has_remnant > HAS_REMNANT_THRESHOLD
            ):
                if far < FAR_THRESHOLD_CENTURY:
                    status["status"] = "go_deep"
                    reason.append(f"FAR < 1/century ({far_yearly:.1f}/year)")
                    status["reason"] = reason
                    logger.info(f"{event_id}: GO DEEP")

                    return status

                if far < FAR_THRESHOLD_DECADE:
                    status["status"] = "go_wide"
                    reason.append(f"1/century < FAR < 1/decade ({far_yearly:.1f}/year)")
                    status["reason"] = reason
                    logger.info(f"{event_id}: GO WIDE")

                    return status

            if (
                far < FAR_THRESHOLD_YEAR
                and combined_ns > PNS_THRESHOLD_DELIBERATE
                and has_ns > HAS_NS_THRESHOLD_DELIBERATE
            ):
                status["status"] = "deliberate"
                reason.append(f"FAR < 1/year ({far_yearly:.1f}/year)")
                status["reason"] = reason
                logger.info(f"{event_id}: DELIBERATE")

                return status

            else:
                status["status"] = "nogo"
                if far >= FAR_THRESHOLD_YEAR:
                    reason.append(f"FAR >= 1/year ({far_yearly:.1f}/year)")
                if has_ns <= HAS_NS_THRESHOLD_DELIBERATE:
                    reason.append(
                        f"hasNS <= {HAS_NS_THRESHOLD_DELIBERATE} ({has_ns:.2f})"
                    )
                if combined_ns <= PNS_THRESHOLD_DELIBERATE:
                    reason.append(
                        f"pNS <= {PNS_THRESHOLD_DELIBERATE} ({combined_ns:.2f})"
                    )
                status["reason"] = reason

                logger.info(f"{event_id}: NO GO")

                return status

            return status

        elif pipeline == "Burst":
            if far < FAR_THRESHOLD_CENTURY:
                status["status"] = "go_deep"
                reason.append(f"FAR < 1/century ({far_yearly:.1f}/year)")
                status["reason"] = reason
                logger.info(f"{event_id}: GO DEEP")

                return status

            if far < FAR_THRESHOLD_DECADE:
                status["status"] = "go_wide"
                reason.append(f"1/century < FAR < 1/decade ({far_yearly:.1f}/year)")
                status["reason"] = reason
                logger.info(f"{event_id}: GO WIDE")

                return status

            if far < FAR_THRESHOLD_YEAR:
                status["status"] = "deliberate"
                reason.append(f"FAR < 1/year ({far_yearly:.1f}/year)")
                status["reason"] = reason
                logger.info(f"{event_id}: DELIBERATE")

                return status

            else:
                status["status"] = "nogo"
                reason.append(f"FAR >= 1/year ({far_yearly:.1f}/year)")
                status["reason"] = reason

                logger.info(f"{event_id}: NO GO")

                return status

            return status

        else:
            return None

    else:
        logger.info(f"Empty record. Discarding")
        return None


def post_on_slack(
    decision: dict | None, slack_client: WebClient, debug: bool = False
) -> None:
    """
    Post the decision on Slack
    """
    pipeline = decision["pipeline"]
    url = decision["url"]

    if decision is None:
        return None

    if decision["status"] == "nogo":
        text = f"*{decision['id']} ({pipeline} event): NO GO*\n"
        if url is not None:
            text += f"<{url}|View event>\n"
        reason = decision["reason"]
        if len(reason) > 0:
            text += "Reason: "
            for r in reason:
                text += f"{r}    "

    if decision["status"] == "deliberate":
        text = f"*{decision['id']} ({pipeline} event): DELIBERATE*\nDoes not warrant an automatic Go, but it needs to be discussed if ToO or serendipitous coverage is the right strategy (based on localization and parameters).\n"
        if url is not None:
            text += f"<{url}|View event>\n"

        if pipeline == "CBC":
            text += f"Parameters: \nFAR: {decision['FAR']:.2E}\np(NS): {decision['pNS']:.2f}\nHas Remnant: {decision['hasRemnant']:.2f}"
        if pipeline == "Burst":
            text += f"Parameters: \nFAR: {decision['FAR']:.2E}"

    if decision["status"] == "go_wide":
        text = f"*{decision['id']} ({pipeline} event): GO WIDE*\n"
        if url is not None:
            text += f"<{url}|View event>\n"
        reason = decision["reason"]
        if len(reason) > 0:
            text += "Reason: "
            for r in reason:
                text += f"{r}    "

    if decision["status"] == "go_deep":
        text = f"*{decision['id']} ({pipeline} event): GO DEEP*\n"
        if url is not None:
            text += f"<{url}|View event>\n"
        reason = decision["reason"]
        if len(reason) > 0:
            text += "Reason: "
            for r in reason:
                text += f"{r}    "

    if debug:
        logger.info(f"Would post on Slack:\n{text}")

    if not debug:
        logger.info(f"Posting on Slack:\n{text}")
        slack_client.chat_postMessage(channel="#go-nogo", text=text)

    return None


def event_exists(event_id) -> bool:
    """
    Check if the event has already been processed
    """
    event_file = Path(__file__).parents[0] / "events" / f"{event_id}.json"

    if event_file.is_file():
        logger.info("Event has already been processed, skipping.")
        return True
    else:
        return False


def save_event(record: dict) -> None:
    """
    Save the event as json
    """
    event_id = record["event_id"]

    event_dir = Path(__file__).parents[0] / "events"
    event_dir.mkdir(exist_ok=True, parents=True)

    event_file = event_dir / f"{event_id}.json"

    with open(event_file, "w") as f:
        json.dump(record, f)

    logger.info(f"Saved event to {event_file}")

    return None


def check_credentials():
    if (
        LVK_GCN_TOKEN is None
        or LVK_GCN_ID is None
        or SLACK_TOKEN is None
        or LVK_GCN_GROUP is None
    ):
        raise ValueError(
            f"You need to export 'LVK_GCN_ID', 'LVK_GCN_GROUP' and 'LVK_GCN_TOKEN', as well as 'SLACK_TOKEN'"
        )


if __name__ == "__main__":
    slack_client = WebClient(token=SLACK_TOKEN)

    parser = argparse.ArgumentParser(
        description="Used to obtain forced photometry for selection of SNe in parallel"
    )
    parser.add_argument("-debug", "-d", action="store_true", help="Run in Debug mode")

    cli = parser.parse_args()

    check_credentials()

    if cli.debug:
        config = {"group.id": "", "auto.offset.reset": "earliest"}

    else:
        config = {"group.id": LVK_GCN_GROUP, "auto.offset.reset": "earliest"}

    consumer = Consumer(
        config=config,
        client_id=LVK_GCN_ID,
        client_secret=LVK_GCN_TOKEN,
    )
    consumer.subscribe(["igwn.gwalert"])

    logger.info("Listening to Kafka stream")

    while True:
        for message in consumer.consume(timeout=1):
            logger.info("Got event")
            consumer.commit(message)
            record = parse_notice(record_raw=message.value())

            if cli.debug:
                if record is not None:
                    decision = decide(record=record)
                    print(decision)
                    post_on_slack(
                        decision=decision, slack_client=slack_client, debug=True
                    )
            else:
                if (
                    record is not None
                    and event_exists(event_id=record["event_id"]) is False
                ):
                    save_event(record=record)
                    decision = decide(record=record)
                    post_on_slack(
                        decision=decision, slack_client=slack_client, debug=False
                    )
