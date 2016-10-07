#!/usr/bin/python
import MySQLdb
db = MySQLdb.connect("localhost","root","mips","mips")
cursor = db.cursor()

query = """truncate table mips.qlty_msr_prfmnc_scre_dtl"""

sql = "select a.tin,\
              a.npi,\
              a.submsn_mthd_id,\
              a.msr_rfrnc_id,\
              a.pgm_yr_id,\
              a.msr_prfmnc_rate,\
              a.msr_type_cd,\
              a.msr_pts,\
              a.msr_wt,\
             (a.msr_pts * a.msr_wt) msr_adjst_pts, \
             (a.msr_wt * 10) msr_tot_psbl_pts,\
              a.submsn_data_id\
             from \
                                       (select  qsd.tin,\
                                                   qsd.npi,\
                                               qsd.submsn_mthd_id,\
                                               qsd.MSR_RFRNC_ID,\
                                               qsd.pgm_yr_id,\
                                               qsd.MSR_PRFMNC_RATE,\
                                               qmr.MSR_TYPE_CD,\
                                                       round(qsd.MSR_PRFMNC_RATE/10) msr_pts,\
                                               case when qmr.msr_type_cd ='OUTCOME' then\
                                                   2 \
                                                else 1 end msr_wt,         \
                                                qsd.SUBMSN_DATA_ID   \
                                         from\
                                       mips.qlty_msr_submsn_data_stng  qsd,\
                                       mips.qlty_msr_rfrnc qmr\
                                       where qsd.msr_rfrnc_id = qmr.msr_rfrnc_id) a"


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
       msr_rfrnc_id      = row[3]
       pgm_yr_id         = row[4]
       msr_prfmnc_rate   = row[5]
       msr_type_cd       = row[6]
       msr_pts           = row[7]
       msr_wt            = row[8]      
       msr_adjst_pts     = row[9]
       msr_tot_psbl_pts  = row[10]      
       number = number + 1
       submsn_data_id    = row[11]
      
      
       cursor.execute('''INSERT into mips.qlty_msr_prfmnc_scre_dtl (tin, npi, submsn_mthd_id, msr_rfrnc_id, pgm_yr_id,msr_prfmnc_rate,msr_type_cd, msr_pts, msr_wt, msr_adjst_pts,msr_tot_psbl_pts,prfmnc_scre_id,submsn_data_id)
                  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(tin, npi, submsn_mthd_id, msr_rfrnc_id, pgm_yr_id,msr_prfmnc_rate, msr_type_cd, msr_pts, msr_wt, msr_adjst_pts,msr_tot_psbl_pts,number,submsn_data_id))
        
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
