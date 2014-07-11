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

import unittest
from edda.filters.balancer import *
from datetime import datetime


class test_balancer(unittest.TestCase):
    def test_criteria(self):
        """test the criteria() method of this module"""
        # invalid messages
        assert criteria("invalid message") == -1
        assert criteria("[Balancer]") == -1

        # valid messages
        assert criteria("Mon Jun 23 10:48:47.123 [Balancer] starting new"
                        " replica set monitor for replica set a with seed"
                        " of 172.19.19.161:27018,172.19.19.162:27018,"
                        "172.19.19.163:27018") == 0
        assert criteria("Mon Jun 23 10:48:47.706 [Balancer] *** start "
                        "balancing round") == 1
        assert criteria("Mon Jun 23 10:48:47.981 [Balancer] *** end "
                        "of balancing round") == 2
        assert criteria("Mon Jun 23 10:48:47.706 [Balancer] distributed "
                        "lock 'balancer/jjezek-saio:30999:1403513327:"
                        "1804289383' acquired, ts : "
                        "53a7e9ef2c2dc81a4b42e7e8") == 3
        assert criteria("Mon Jun 23 10:48:48.115 [Balancer] distributed "
                        "lock 'balancer/jjezek-saio:30999:1403513327:"
                        "1804289383' unlocked.") == 4

    def test_process(self):
        """test the process method of this module"""
        date = datetime.now()
        members = ["172.19.19.161:27018", "172.19.19.162:27018", 
                   "172.19.19.163:27018"]
        # invalid messages
        assert process("This should fail", date) == None
        assert process("[Balancer]", date) == None

        # valid messages
        self.check_state("Mon Jun 23 10:48:47.123 [Balancer] starting "
                         "new replica set monitor for replica set a "
                         "with seed of 172.19.19.161:27018,172.19.19."
                         "162:27018,172.19.19.163:27018", 0, date, "a", 
                         members, "new_shard", "", "")
        self.check_state("Mon Jun 23 10:48:47.706 [Balancer] *** start "
                         "balancing round", 1, date, "", [], 
                         "start_balancing_round", "", "")
        self.check_state("Mon Jun 23 10:48:47.981 [Balancer] *** end "
                         "of balancing round", 2, date, "", [], 
                         "end_balancing_round", "", "")
        self.check_state("Mon Jun 23 10:48:47.706 [Balancer] distributed "
                         "lock 'balancer/jjezek-saio:30999:1403513327:"
                         "1804289383' acquired, ts : "
                         "53a7e9ef2c2dc81a4b42e7e8", 3, date, "", [], 
                         "balancer_lock", 
                         "balancer/jjezek-saio:30999:1403513327:"
                         "1804289383", 
                         "53a7e9ef2c2dc81a4b42e7e8")
        self.check_state("Mon Jun 23 10:48:48.115 [Balancer] distributed "
                         "lock 'balancer/jjezek-saio:30999:1403513327:"
                         "1804289383' unlocked.", 4, date, "", [], 
                         "balancer_unlock",
                         "balancer/jjezek-saio:30999:1403513327:"
                         "1804289383", "")

    def check_state(self, message, code, date, replSet, members, 
                    subtype, lockName, ts):
        """helper method for test_process"""
        doc = process(message, date)
        assert doc
        assert doc["type"] == "balancer"
        assert doc["msg"] == message
        assert doc["info"]["subtype"] == subtype
       
        if code == 0:
            assert doc["info"]["replSet"] == replSet
            assert set(doc["info"]["members"]) == set(members)
            assert doc["info"]["server"] == "self"
        
        # No additional tests for codes 1 and 2

        elif code == 3:
            assert doc["info"]["lockName"] == lockName
            assert doc["info"]["ts"] == ts
 
        elif code == 4:
            assert doc["info"]["lockName"] == lockName
      

if __name__ == '__main__':
    unittest.main()
