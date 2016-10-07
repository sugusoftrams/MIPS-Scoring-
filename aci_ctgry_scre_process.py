#!/usr/bin/python
import MySQLdb
db = MySQLdb.connect("localhost","root","mips","mips")
cursor = db.cursor()

query = """truncate table mips.aci_catgry_scre""" 

sql = " select  aci_prfmnc_id,\
        aci_submsn_mthd_id,tin,\
        npi,((p.tot_adjstd_pts) + isnull(p.tot_bonus_pts) ) tot_psbl_pts,\
        (p.tot_adjstd_pts) tot_adjstd_pts,\
        (p.tot_bonus_pts) tot_bonus_pts,\
        case when ((p.tot_adjstd_pts) + isnull(p.tot_bonus_pts) ) > 100 then 100 else ((p.tot_adjstd_pts) + isnull(p.tot_bonus_pts) ) end aci_prfmnc_catgry_scre,\
        case when ((p.tot_adjstd_pts) + isnull(p.tot_bonus_pts) ) > 100 then 25 else \
        (((p.tot_adjstd_pts) + isnull(p.tot_bonus_pts) ) * .25 ) end tot_cps_pts\
        from  (select aci_prfmnc_id,aci_submsn_mthd_id, tin,  npi,sum((msr_prfmnc_pts)) tot_adjstd_pts,\
        case when sum(isnull(msr_base_pts)) > 60 then 60 else sum(isnull(msr_base_pts)) end tot_bonus_pts, pgm_yr_id  from mips.aci_peformance_scre_dtl \
        group by  tin, npi,aci_prfmnc_id,aci_submsn_mthd_id, pgm_yr_id) p "


try:
    cursor.execute(query)
    db.commit()
    
    cursor.execute(sql)
    results = cursor.fetchall()

    
    number = 0
    for row in results:
        
       aci_prfmnc_id          = row[0]
       aci_submsn_mthd_id     = row[1]
       tin                    = row[2]
       npi                    = row[3]       
       tot_psbl_pts           = row[4]
       tot_adjstd_pts         = row[5]
       tot_bonus_pts          = row[6]       
       aci_prfmnc_catgry_scre = row[7]
       tot_cps_pts            = row[8]
       #pgm_yr_id              = row[9]
       number = number + 1
       
      
      
       cursor.execute('''INSERT into mips.aci_catgry_scre (aci_prfmnc_id,aci_submsn_mthd_id ,aci_catgry_scre_id,tin, npi,tot_psbl_pts,tot_adjstd_pts,tot_bonus_pts,aci_prfmnc_catgry_scre,tot_cps_pts ,pgm_yr_id )
                  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(aci_prfmnc_id,aci_submsn_mthd_id ,number,tin, npi,tot_psbl_pts,tot_adjstd_pts,tot_bonus_pts,aci_prfmnc_catgry_scre,tot_cps_pts ,1))
        

       
    db.commit()
except RuntimeError as e:
    print e
except TypeError as te:
    print te
except NameError as ne:
    print ne
    
db.close()
