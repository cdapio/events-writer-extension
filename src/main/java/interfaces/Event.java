/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package interfaces;

public class Event {
    String event_type;
    String event_publish_time;
    String version;
    String instance_name;
    String project_name;
    ProgramEvent program_event;

    public Event(String event_type, String event_publish_time, String version, String instance_name, String project_name, ProgramEvent program_event) {
        this.event_type = event_type;
        this.event_publish_time = event_publish_time;
        this.version = version;
        this.instance_name = instance_name;
        this.project_name = project_name;
        this.program_event = program_event;
    }
}
