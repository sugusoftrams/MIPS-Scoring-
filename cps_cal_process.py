#!/usr/bin/python
import MySQLdb
db = MySQLdb.connect("localhost","root","mips","mips")
cursor = db.cursor()

query = """truncate table mips.cps_catgry_scre_dtl""" 
   
    
sql = "select  distinct a.tin,a.npi,\
       sum( a.qlty_catgry_scre) qlty_catgry_scre, sum( a.cpia_scre) cpia_catgry_scre, sum(a.aci_scre) aci_catgry_scre,\
       sum( a.qlty_catgry_scre) + sum( a.cpia_scre) + sum(a.aci_scre) tot_cps_pts from \
            (\
                select tin,npi, min(submsn_hirchy) rank ,tot_cps_pts qlty_catgry_scre, 0 cpia_scre, 0 aci_scre\
                from mips.qlty_msr_ctgry_scre_dtl qcd,\
                     mips.qlty_msr_submsn_mthd qsm\
                where qcd.submsn_mthd_id =    qsm.submsn_mthd_id\
                group by tin,npi\
                 union\
                select tin,npi, min(submsn_hirchy) rank , 0 qlty_catgry_scre, tot_cps_pts cpia_scre,0 aci_scre\
                from mips.cpia_actvty_ctgry_scre ccs,\
                     mips.cpia_submsn_mthd csm\
                where  ccs.cpia_subms_mthd_id =    csm.cpia_subms_mthd_id\
                group by tin,npi \
              union\
                select tin,npi, min(submsn_hirchy) rank , 0 qlty_catgry_scre,0  cpia_scre, tot_cps_pts aci_scre\
                from mips.aci_catgry_scre acs,\
                     mips.aci_submsn_mthd asm\
                where  acs.aci_submsn_mthd_id =    asm.aci_submsn_mthd_id\
                group by tin,npi ) a  group by a.tin,a.npi "


try:
    cursor.execute(query)
    db.commit()
    
    cursor.execute(sql)
    results = cursor.fetchall()

    

    for row in results:
        
       tin                    = row[0]
       npi                    = row[1]       
       qlty_catgry_scre       = row[2]
       cpia_catgry_scre       = row[3]
       aci_catgry_scre        = row[4]       
       tot_cps_pts            = row[5]
           
      
       cursor.execute('''INSERT into mips.cps_catgry_scre_dtl (tin, npi,qlty_catgry_scre,cpia_catgry_scre,aci_catgry_scre,tot_cps_pts)
                  values (%s,%s,%s,%s,%s,%s)''',(tin, npi,qlty_catgry_scre,cpia_catgry_scre,aci_catgry_scre,tot_cps_pts))
        

       
    #db.commit()
except RuntimeError as e:
    print e
except TypeError as te:
    print te
except NameError as ne:
    print ne
    
db.close()
