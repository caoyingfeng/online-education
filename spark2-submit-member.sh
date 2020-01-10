#! /bin/bash
spark2-submit --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 2g --class com.cy.memeber.controller.DwdMemberController --queue spark com_cy_warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar

spark2-submit --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 2g --class com.cy.memeber.controller.DwsMemberController --queue spark com_cy_warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar

spark2-submit --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 2g --class com.cy.memeber.controller.AdsMemberController --queue spark com_cy_warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar