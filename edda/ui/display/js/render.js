// Copyright 2014 MongoDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/* Render the cluster at a single point in time */
var renderFrame = function(time) {
    if (!frames[time]) return;
    // TODO: could we be more efficient than wiping everything every time?
    clearLayer("arrow");
    clearLayer("server");

    renderLinks(frames[time], contexts["arrow"]);
    renderBrokenLinks(frames[time], contexts["arrow"]);
    renderSyncs(frames[time], contexts["arrow"]);
    drawServers(frames[time], contexts["server"]);
 };

/* Render broken links between servers at a single point in time */
var renderBrokenLinks = function(frame, ctx) {
    for (var server in frame["broken_links"]) {
        var list = frame["broken_links"][server];
        for (i = 0; i < list.length; i++) {
            drawBrokenLink(servers[server]["x"], servers[server]["y"],
                        servers[list[i]]["x"], servers[list[i]]["y"], ctx);
        }
    }
};

/* Render links between servers for a single point in time */
var renderLinks = function(frame, ctx) {
    for (var server in frame["links"]) {
        var list = frame["links"][server];
        for (i = 0; i < list.length; i++) {
            drawOneLine(servers[server]["x"], servers[server]["y"],
                     servers[list[i]]["x"], servers[list[i]]["y"], ctx);
        }
    }
};

/* Render syncs between servers for a single point in time */
var renderSyncs = function(frame, ctx) {
    for (var server in frame["syncs"]) {
        var list = frame["syncs"][server];
        for (i = 0; i < list.length; i++) {
            drawOneArrow(servers[list[i]]["x"], servers[list[i]]["y"],
                      servers[server]["x"], servers[server]["y"], ctx);
        }
    }
};

/* Clear the specified canvas layer */
var clearLayer = function(name) {
    canvases[name].width = canvases[name].width;
};
