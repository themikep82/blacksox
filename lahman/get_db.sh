curl -o lahman.zip http://seanlahman.com/files/database/lahman2016-sql.zip
unzip lahman.zip
rm readme2016.txt
sed -i -e '1,24d' lahman2016.sql
sed -i -e "1iSET search_path TO lahman;" lahman2016.sql
sed -i -e "1iCREATE SCHEMA IF NOT EXISTS lahman;" lahman2016.sql
sed -i -e "1iDROP SCHEMA IF EXISTS lahman CASCADE;" lahman2016.sql
sed -i -e "s/\`/\"/g" lahman2016.sql
sed -i -e "s/\`/\"/g" lahman2016.sql
sed -i -e "s/int(11)/int/g" lahman2016.sql
sed -i -e "s/ENGINE=InnoDB DEFAULT CHARSET=utf8//g" lahman2016.sql
sed -i -e "s/Twp;/Twp,/g" lahman2016.sql
sed -i -e "s/Township;/Township,/g" lahman2016.sql
sed -i -e "s/Arlington;/Arlington,/g" lahman2016.sql
sed -i -e "s/Grounds;/Grounds,/g" lahman2016.sql
sed -i -e "s/Park;/Park,/g" lahman2016.sql
sed -i -e "s/Field;/Field,/g" lahman2016.sql
sed -i -e "s/Stadium;/Stadium,/g" lahman2016.sql
sed -i -e "s/SET FOREIGN_KEY_CHECKS = 1;//g" lahman2016.sql
mv lahman2016.sql lahmanready.sql