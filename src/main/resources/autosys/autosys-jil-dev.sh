update_job: ihb.sto.sit.edh.cm.purge.gn1
job_type: CMD
box_name:
description: Run Cluster Managemnet Purge for Group 1 (Monday)
command: /opt/ihb1btch/cluster-management/bin/purging.sh processing_group=1
date_conditions: 1
days_of_week: mo
start_times: "19:00"
std_out_file: /opt/ihb1btch/logs/autosys_out_cm_purge_1.log
std_err_file: /opt/ihb1btch/logs/autosys_err_cm_purge_1.log
machine: devecpvm010200
owner: ihb1btch
alarm_if_fail: 1
alarm_if_terminated: 1

update_job: ihb.sto.sit.edh.cm.purge.gn1
job_type: CMD
box_name:
description: Run Cluster Managemnet Purge for Group 2 (Tuesday)
command: /opt/ihb1btch/cluster-management/bin/purging.sh processing_group=2
date_conditions: 1
days_of_week: tu
start_times: "19:00"
std_out_file: /opt/ihb1btch/logs/autosys_out_cm_purge_2.log
std_err_file: /opt/ihb1btch/logs/autosys_err_cm_purge_2.log
machine: devecpvm010200
owner: ihb1btch
alarm_if_fail: 1
alarm_if_terminated: 1

update_job: ihb.sto.sit.edh.cm.purge.gn1
job_type: CMD
box_name:
description: Run Cluster Managemnet Purge for Group 3 (Wednesday)
command: /opt/ihb1btch/cluster-management/bin/purging.sh processing_group=3
date_conditions: 1
days_of_week: we
start_times: "19:00"
std_out_file: /opt/ihb1btch/logs/autosys_out_cm_purge_3.log
std_err_file: /opt/ihb1btch/logs/autosys_err_cm_purge_3.log
machine: devecpvm010200
owner: ihb1btch
alarm_if_fail: 1
alarm_if_terminated: 1

update_job: ihb.sto.sit.edh.cm.purge.gn1
job_type: CMD
box_name:
description: Run Cluster Managemnet Purge for Group 4 (Thursday)
command: /opt/ihb1btch/cluster-management/bin/purging.sh processing_group=4
date_conditions: 1
days_of_week: th
start_times: "19:00"
std_out_file: /opt/ihb1btch/logs/autosys_out_cm_purge_4.log
std_err_file: /opt/ihb1btch/logs/autosys_err_cm_purge_4.log
machine: devecpvm010200
owner: ihb1btch
alarm_if_fail: 1
alarm_if_terminated: 1

update_job: ihb.sto.sit.edh.cm.purge.gn1
job_type: CMD
box_name:
description: Run Cluster Managemnet Purge for Group 5 (Friday)
command: /opt/ihb1btch/cluster-management/bin/purging.sh processing_group=5
date_conditions: 1
days_of_week: fr
start_times: "19:00"
std_out_file: /opt/ihb1btch/logs/autosys_out_cm_purge_5.log
std_err_file: /opt/ihb1btch/logs/autosys_err_cm_purge_5.log
machine: devecpvm010200
owner: ihb1btch
alarm_if_fail: 1
alarm_if_terminated: 1

update_job: ihb.sto.sit.edh.cm.purge.gn1
job_type: CMD
box_name:
description: Run Cluster Managemnet Purge for Group 6 (Monday)
command: /opt/ihb1btch/cluster-management/bin/purging.sh processing_group=6
date_conditions: 1
days_of_week: sa
start_times: "19:00"
std_out_file: /opt/ihb1btch/logs/autosys_out_cm_purge_6.log
std_err_file: /opt/ihb1btch/logs/autosys_err_cm_purge_6.log
machine: devecpvm010200
owner: ihb1btch
alarm_if_fail: 1
alarm_if_terminated: 1

update_job: ihb.sto.sit.edh.cm.purge.gn1
job_type: CMD
box_name:
description: Run Cluster Managemnet Purge for Group 7 (Monday)
command: /opt/ihb1btch/cluster-management/bin/purging.sh processing_group=7
date_conditions: 1
days_of_week: su
start_times: "19:00"
std_out_file: /opt/ihb1btch/logs/autosys_out_cm_purge_7.log
std_err_file: /opt/ihb1btch/logs/autosys_err_cm_purge_7.log
machine: devecpvm010200
owner: ihb1btch
alarm_if_fail: 1
alarm_if_terminated: 1



