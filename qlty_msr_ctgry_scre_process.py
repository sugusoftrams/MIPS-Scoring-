#!/usr/bin/python
import MySQLdb
db = MySQLdb.connect("localhost","root","mips","mips")
cursor = db.cursor()


query = """truncate table mips.qlty_msr_ctgry_scre_dtl"""

sql = "select  a.tin,\
               a.npi,\
               a.submsn_mthd_id,\
               a.pgm_yr_id,\
               a.tot_adjst_pts,\
               a.tot_psbl_pts,\
               a.tot_bonus_pts,(((a.tot_adjst_pts + a.tot_bonus_pts)/ a.tot_psbl_pts) * 100) PRFMNC_CTGRY_PCTG,\
               ((((a.tot_adjst_pts + a.tot_bonus_pts)/ a.tot_psbl_pts))  * 0.50) tot_cps_pts from \
              (select tin,\
                           npi,\
                           submsn_mthd_id,\
                           pgm_yr_id,\
                           sum(msr_adjst_pts) tot_adjst_pts,\
                           sum(msr_tot_psbl_pts) tot_psbl_pts,\
                           case when sum(case when msr_type_cd ='OUTCOME' then 1 else 0 end)  >= 1 then 2 else 0 end tot_bonus_pts       \
                            from  mips.qlty_msr_prfmnc_scre_dtl\
                        GROUP BY TIN,\
                           npi,\
                           submsn_mthd_id,\
                           pgm_yr_id ) a "


try:
    cursor.execute(query)
    db.commit()
    
    cursor.execute(sql)
    results = cursor.fetchall()
    
    number = 0
    for row in results:
        
       tin               = row[0]
       npi               = row[1]
       submsn_mthd_id    = row[2]
       pgm_yr_id         = row[3]
       tot_adjst_pts     = row[4]
       tot_psbl_pts      = row[5]
       tot_bonus_pts     = row[6]
       prfmnc_ctgry_pctg = row[7]
       tot_cps_pts       = row[8]  
       number = number + 1
 
      
       cursor.execute('''INSERT into mips.qlty_msr_ctgry_scre_dtl (tin, npi, submsn_mthd_id, pgm_yr_id,tot_adjst_pts,tot_psbl_pts,tot_bonus_pts,prfmnc_ctgry_pctg, tot_cps_pts,CTGRY_SCRE_ID)
                  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(tin, npi, submsn_mthd_id, pgm_yr_id,tot_adjst_pts,tot_psbl_pts,tot_bonus_pts,prfmnc_ctgry_pctg, tot_cps_pts,number))
        
    ##   print "tin=%s,npi=%s,msr_rfrnc_id=%d,pgm_yr_id=%s,submsn_mthd_id=%d,msr_type_cd=%d,msr_prfmnc_rate=%d,msr_wt=%d,msr_pts=%d,msr_adjst_pts=%d,msr_tot_psbl_pts=%d" % \
      ##       ( TIN , NPI , MSR_RFRNC_ID ,PGM_YR_ID , SUBMSN_MTHD_ID, msr_type_cd,msr_prfmnc_rate,msr_wt,msr_pts,msr_adjst_pts,msr_tot_psbl_t )
       
    db.commit()
except RuntimeError as e:
    print e
except TypeError as te:
    print te
except NameError as ne:
    print ne
    
db.close()
