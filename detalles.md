Tengo la siguiente tabla llamada `powerful-star-421901.database.iron` en bigquery

ID	STRING	REQUIRED
Age	STRING	NULLABLE
Gender	STRING	NULLABLE
Occupation	STRING	NULLABLE
line_of_work	STRING	NULLABLE
time_bp	NUMERIC	NULLABLE
time_dp	NUMERIC	NULLABLE
travel_time	NUMERIC	NULLABLE
easeof_online	NUMERIC	NULLABLE
home_env	INTEGER	NULLABLE
prod_inc	NUMERIC	NULLABLE
sleep_bal	NUMERIC	NULLABLE
new_skill	NUMERIC	NULLABLE
fam_connect	NUMERIC	NULLABLE
relaxed	NUMERIC	NULLABLE
self_time	NUMERIC	NULLABLE
like_hw	NUMERIC	NULLABLE
dislike_hw	NUMERIC	NULLABLE
prefer	STRING	NULLABLE
certaindays_hw	STRING	NULLABLE
travel_work	STRING	NULLABLE
loaded	BOOLEAN	REQUIRED

Necesito hacer una query schedulada que pase estos datos filtrados a otra tabla llamada `powerful-star-421901.database.emerald`

necesito que tenga la siguiente estructura:

ID	STRING	REQUIRED (el mismo valor)
Age	STRING	REQUIRED (el mismo valor)
Gender	STRING	REQUIRED (el mismo valor)
Occupation	STRING	REQUIRED (el mismo valor)
line_of_work	STRING	REQUIRED (el mismo valor pero si esta vacio o null que lo reemplace por el string "N/A")
time_bp	NUMERIC	REQUIRED (el mismo valor)
time_dp	NUMERIC	REQUIRED (el mismo valor)
travel_time	NUMERIC	REQUIRED (el mismo valor)
easeof_online	NUMERIC	REQUIRED (el mismo valor)
home_env	INTEGER	REQUIRED (el mismo valor)
prod_inc	NUMERIC	REQUIRED (el mismo valor)
sleep_bal	NUMERIC	REQUIRED (el mismo valor)
new_skill	NUMERIC	REQUIRED (el mismo valor)
fam_connect	NUMERIC	REQUIRED (el mismo valor)
relaxed	NUMERIC	REQUIRED (el mismo valor)
self_time	NUMERIC	REQUIRED (el mismo valor)
prefer	STRING	REQUIRED (el mismo valor)
certaindays_hw	STRING	REQUIRED (el mismo valor)
travel_work	STRING	REQUIRED (el mismo valor)

Y que a su vez, con cada registro que de iron que este correcto y se cree en emerald, necesito que ese registro en iron cambie su valor de loaded a true

Esto es posible? y como lo hago desde la consola de gcp
