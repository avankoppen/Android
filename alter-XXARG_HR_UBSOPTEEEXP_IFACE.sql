ALTER TABLE XXARG.XXARG_HR_UBSOPTEEEXP_IFACE ADD
( WORK_JURISDICTION_CODE       VARCHAR2(30),
  WORK_COUNTY_SCHOOL_DISTRICT  VARCHAR2(30),
  WORK_CITY_SCHOOL_DISTRICT    VARCHAR2(30),
  RESI_JURISDICTION_CODE       VARCHAR2(30),
  RESI_COUNTY_SCHOOL_DISTRICT  VARCHAR2(30),
  RESI_CITY_SCHOOL_DISTRICT    VARCHAR2(30),
  WORK_AT_HOME_FLAG            VARCHAR2(30)     DEFAULT 'N'    NOT NULL,
  FORCE_MANUAL_PAYROLL_TRXS    VARCHAR2(1)      DEFAULT 'N'    NOT NULL );