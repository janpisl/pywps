import mappyfile
import psycopg2


d = mappyfile.open('test.map')


d["NAME"] = "testpg1"
d["WEB"]["METADATA"]["wms_title"] = "testpg2"
#d["WEB"]["METADATA"]["wms_onlineresource"] = "http://127.0.0.1/cgi-bin/mapserv?map=/pywps/inout/storage/db/mapserver/testpg.map"
#d["WEB"]["METADATA"]["wms_srs"] = 

d["LAYERS"][0]["NAME"] = "testpg3"
d["LAYERS"][0]["CONNECTIONTYPE"] = "postgis"
d["LAYERS"][0]["CONNECTION"] = "dbname=XXXX host=localhost user=XXXXX password=XXXXXXXX port=5432"
d["LAYERS"][0]["DATA"] = "wkb_geometry from " + "buff_out" #TODO: replace hard coded "buff_out"


# Connect to an existing database
conn = psycopg2.connect("dbname=XXXXXX user=XXXXXX password=XXXXXXXXX host=localhost port=5432")

# Open a cursor to perform database operations
cur = conn.cursor()

# extent
cur.execute("SELECT ST_Extent(wkb_geometry) FROM buff_out;")
fextent = cur.fetchone()
# reformat bbox (from tuple to string, remove brackets, replace "," with a " ") so it can be used in the mapfile
fextent_formatted = fextent[0][fextent[0].find("(")+1:fextent[0].find(")")].replace(",", " ")
d["extent"] = fextent_formatted

# projection / not working
cur.execute("SELECT Find_SRID('public', 'buff_out', 'wkb_geometry');")
fproj = cur.fetchone()


# geometry
cur.execute("SELECT ST_AsText(wkb_geometry) from buff_out;")
fgeom = cur.fetchone()
d["LAYERS"][0]["type"] = fgeom[0].split("(", 1)[0]


# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()

fn = r"C:\Data\mymap.map"
mappyfile.save(d, fn)