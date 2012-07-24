# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python

# anatomy of a clock skew document:
# document = {
#    "type" = "clock_skew"
#    "server_num" = int
#    "partners" = {
#          server_num : {
#                "skew_1" : weight,
#                "skew_2" : weight...
#          }
#     }

from datetime import timedelta
import logging


def server_clock_skew(db, coll_name):
    """ Given the mongodb entries generated by edda,
        attempts to detect and resolve clock skew
        across different servers.
    """
    logger = logging.getLogger(__name__)

    clock_skew = db[coll_name + ".clock_skew"]
    servers = db[coll_name + ".servers"]

    for doc_a in servers.find():
        a_name = doc_a["network_name"]
        a_num = str(doc_a["server_num"])
        if a_name == "unknown":
            logger.debug("Skipping unknown server")
            continue
        skew_a = clock_skew.find_one({"server_num": a_num})
        if not skew_a:
            skew_a = clock_skew_doc(a_num)
            clock_skew.save(skew_a)
        for doc_b in servers.find():
            b_name = doc_b["network_name"]
            b_num = str(doc_b["server_num"])
            if b_name == "unknown":
                logger.debug("Skipping unknown server")
                continue
            if a_name == b_name:
                logger.debug("Skipping identical server")
                continue
            if b_num in skew_a["partners"]:
                logger.debug("Clock skew already found for this server")
                continue
            logger.info("Finding clock skew "
                "for {0} - {1}...".format(a_name, b_name))
            skew_a["partners"][b_num] = detect(a_name, b_name, db, coll_name)
            if not skew_a["partners"][b_num]:
                continue
            skew_b = clock_skew.find_one({"server_num": b_num})
            if not skew_b:
                skew_b = clock_skew_doc(b_num)
            # flip according to sign convention for other server:
            # if server is ahead, +t
            # if server is behind, -t
            skew_b["partners"][a_num] = {}
            for t in skew_a["partners"][b_num]:
                wt = skew_a["partners"][b_num][t]
                t = str(-int(t))
                logger.debug("flipped one")
                skew_b["partners"][a_num][t] = wt
            clock_skew.save(skew_a)
            clock_skew.save(skew_b)


def detect(a, b, db, coll_name):
    """ Compares each entry from cursor_a against every entry from
        cursor_b.  In the case of matching messages, advances both cursors.
        Calculates time skew.  While entries continue to match, adds
        weight to that time skew value.  Stores all found time skew values,
        with respective weights, in a dictionary and returns.
        KNOWN BUGS: this algorithm may count some matches twice.
    """

    entries = db[coll_name + ".entries"]

    # set up cursors
    cursor_a = entries.find({
        "type": "status",
        "origin_server": a,
        "info.server": b
    })

    cursor_b = entries.find({
        "type": "status",
        "origin_server": b,
        "info.server": "self"
    })
    cursor_a.sort("date")
    cursor_b.sort("date")
    logger = logging.getLogger(__name__)
    skews = {}
    list_a = []
    list_b = []

    # store the entries from the cursor in a list
    for a in cursor_a:
        list_a.append(a)
    for b in cursor_b:
        list_b.append(b)

    # for each a, compare against every b
    for i in range(0, len(list_a)):
        for j in range(0, len(list_b)):
           # if they match, crawl through and count matches
            if match(list_a[i], list_b[j]):
                wt = 0
                while match(list_a[i + wt], list_b[j + wt]):
                    wt += 1
                    if (wt + i >= len(list_a)) or (wt + j >= len(list_b)):
                        break
                # calculate time skew, save with weight
                td = list_b[j + wt - 1]["date"] - list_a[i + wt - 1]["date"]
                td = timedelta_to_int(td)
                if abs(td) > 2:
                    key = in_skews(td, skews)
                    if not key:
                        logger.debug(("inserting new weight "
                            "for td {0} into skews {1}").format(td, skews))
                        skews[str(td)] = wt
                    else:
                        logger.debug(
                            " adding additional weight for "
                            "td {0} into skews {1}".format(td, skews))
                        skews[key] += wt
                # could maybe fix redundant counting by taking
                # each a analyzed here and comparing against all earlier b's.
                # another option would be to keep a table of
                    # size[len(a)*len(b)] of booleans.
                # or, just accept this bug as something that weights multiple
                # matches in a row even higher.

    return skews


def match(a, b):
    """ Given two entries, determines whether
        they match.  For now, only handles pure status messages.
    """
    if a["info"]["state_code"] == b["info"]["state_code"]:
        return True
    return False


def in_skews(t, skews):
    """ If this entry is not close in value
        to an existing entry in skews, return None.
        If it is close in value to an existing entry,
        return the key for that entry.
    """
    for skew in skews:
        if abs(int(skew) - t) < 2:
            return skew
    return None


def timedelta_to_int(td):
    """ Takes a timedelta and converts it
        to a single string that represents its value
        in seconds.  Returns a string.
    """
    # because MongoDB cannot store timedeltas
    sec = 0
    t = abs(td)
    sec += t.seconds
    sec += (86400 * t.days)
    if td < timedelta(0):
        sec = -sec
    return sec


def clock_skew_doc(num):
    """ Create and return an empty clock skew doc
        for this server.
    """
    doc = {}
    doc["server_num"] = num
    doc["type"] = "clock_skew"
    doc["partners"] = {}
    return doc
