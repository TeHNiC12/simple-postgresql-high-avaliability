#!/bin/bash

(sleep 5 && /etc/ha_controller/postgresql_ha_controller.sh) &
(sleep 15 && /etc/ha_controller/postgresql_ha_controller.sh) &
(sleep 25 && /etc/ha_controller/postgresql_ha_controller.sh) &
(sleep 35 && /etc/ha_controller/postgresql_ha_controller.sh) &
(sleep 45 && /etc/ha_controller/postgresql_ha_controller.sh) &
(sleep 55 && /etc/ha_controller/postgresql_ha_controller.sh) &