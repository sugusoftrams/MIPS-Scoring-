#!/usr/bin/python
import MySQLdb
db = MySQLdb.connect("localhost","root","mips","mips")
cursor = db.cursor()

query = """truncate table mips.cpia_actvty_ctgry_scre"""

sql = "select  p.tin,\
        p.npi,        \
        p.cpia_subms_mthd_id,\
        p.pgm_yr_id,\
        p.tot_psbl_pts,\
        p.tot_adjstd_pts,\
        (p.tot_adjstd_pts/p.tot_psbl_pts) * 100 prfmnc_ctgry_pctg  ,\
        ((p.tot_adjstd_pts/p.tot_psbl_pts) * 100) *   0.15 tot_cps_pts\
        from \
(select  tin,\
        npi, cpia_subms_mthd_id,\
        pgm_yr_id,\
	60 tot_psbl_pts,\
        case when sum(cpia_actvty_adjst_pts) > 60 then 60 else sum(cpia_actvty_adjst_pts) end tot_adjstd_pts from mips.cpia_actvty_prfmnc_scre_dtl\
        group by  tin, npi,cpia_subms_mthd_id, pgm_yr_id) p"


try:
    
    cursor.execute(query)
    db.commit()
    
    cursor.execute(sql)
    results = cursor.fetchall()

    
    number = 0
    for row in results:
        
       tin                    = row[0]
       npi                    = row[1]       
       cpia_subms_mthd_id     = row[2]
       pgm_yr_id              = row[3]
       tot_psbl_pts           = row[4]     
       tot_adjstd_pts         = row[5]
       prfmnc_ctgry_pctg      = row[6]
       tot_cps_pts            = row[7]         
       number = number + 1
       
      
      
       cursor.execute('''INSERT into mips.cpia_actvty_ctgry_scre (tin, npi,cpia_subms_mthd_id,pgm_yr_id,tot_psbl_pts,tot_adjstd_pts, prfmnc_ctgry_pctg,tot_cps_pts ,CPIA_CTGRY_SCRE_ID)
                  values (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(tin, npi,cpia_subms_mthd_id,pgm_yr_id,tot_psbl_pts,tot_adjstd_pts, prfmnc_ctgry_pctg,tot_cps_pts,number))
        
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
