INI README 
- PENAMAAN FILE : SNAKE CASE (insert_records.py)
- PENAMAAN FOLDER : FISHBONE (football-data-pipeline)

Data football ini digunakan untuk
- analitik data sepakbola (matches, teams, competitions, pemain (saat ini pemain belum memungkinkan))
- membuat keputusan dan prediksi dari data sepakbola

Dengan data yang diambil berupa data pertandingan, tim, kompetisi, hingga pemain dari tiap 12 liga utama. Data yang diambil merupakan data yang terbaru, sehingga dapat meningkatkan hasil penggunaan dari data ini.
 
Pada football data pipeline ini saya punya arsitektur berikut
- menggunakan python sbagai bahasa utama
- menggunakan postgres 17 sebagai database
- menggunakan dbt untuk transformasi data
- menggunakan airflow untuk orkestrasi pengambilan data
- semua proses berjalan menggunakan docker
- akan ada pembersihan data yang nantinya akan dilakukan setelah semua arsitektur sebelumnya telah selesai

diharapkan dengan adanya dapat data pipeline ini, akan dapat memudahkan penggunaan data yang diambil oleh para analyst dan pekerja data lainnya.
