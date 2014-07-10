# Copyright 2014 MongoDB, Inc.
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

from edda.supporting_methods import capture_address

def criteria(msg):
    """Does the given log line fit the criteria for this filter?
    If yes, return an integer code.  Otherwise, return -1.
    """
    if not '[Balancer]' in msg:
        return -1

    # recognize a shard
    elif 'starting new replica set monitor' in msg:
        return 0

    elif '*** start balancing round' in msg:
        return 1

    elif '*** end of balancing round' in msg:
        return 2

    elif 'distributed lock' in msg:
        if 'acquired' in msg:
            return 3
        elif 'unlocked' in msg:
            return 4

    return -1

def process(msg, date):
    """If the given log line fits the critera for this filter,
    process it and create a document of the following format:
    doc = {
       "date" : date,
       "type" : "balancer",
       "msg" : msg,
       "origin_server" : name,
       "info" : {
          "subtype" : "new_shard",
          "replSet" : name,
          "members" : [ strings of server names ]
          }
    }
    """
    result = criteria(msg)
    if result < 0:
        return None

    doc = {}
    doc["date"] = date
    doc["type"] = "balancer"
    doc["msg"] = msg
    doc["info"] = {}

    if result == 0:
        # get replica set name and seeds
        a = msg.split("starting new replica set monitor for replica set ")
        b = a[1].split()
        doc["info"]["subtype"] = "new_shard"
        doc["info"]["replSet"] = b[0]
        doc["info"]["members"] = b[4].split(',')
        doc["info"]["server"] = "self"
    
    elif result == 1:
        doc["info"]["subtype"] = "start_balancing_round"
        doc["info"]["status"] = "started"

    elif result == 2:
        doc["info"]["subtype"] = "end_balancing_round"
        doc["info"]["status"] = "ended"

    elif result == 3:
        doc["info"]["subtype"] = "balancer_lock"
       # Get lock name 
        a = msg.split(' distributed lock ')[1]
        b = a.split(' acquired, ')
        lockName = b[0]

        # Remove quotes from lock name
        lockName = lockName.lstrip('\'').rstrip('\'')
        doc["info"]["lockName"] = lockName

        # Get ts
        ts = b[1].lstrip('ts : ')
        doc["info"]["ts"] = ts
 
    elif result == 4:
        doc["info"]["subtype"] = "balancer_unlock"
        # Get lock name 
        a = msg.split(' distributed lock ')[1]
        lockName = a.split(' unlocked')[0]

        # Remove quotes from lock name
        lockName = lockName.lstrip('\'').rstrip('\'')

        doc["info"]["lockName"] = lockName
    
    return doc
