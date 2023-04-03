while true
do
   echo "optimizing table"
   python rewrite_tagged_telemetry.py --catalog hogwarts_users_u_ouo --schema jcc
   sleep 3600
done