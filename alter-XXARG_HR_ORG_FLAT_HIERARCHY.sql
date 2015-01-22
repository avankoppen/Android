-- THIS WILL INVALIDATE MANY CUSTOM OBJECTS
--  ONLY DO THIS COMPILATION AFTER HOURS AND BE READY TO COMPILE INVALID OBJECTS AS WELL

ALTER TABLE XXARG.XXARG_HR_ORG_FLAT_HIERARCHY ADD
( LAST_UPDATED_BY          NUMBER            DEFAULT -1           NOT NULL,
  LAST_UPDATE_DATE         DATE              DEFAULT SYSDATE      NOT NULL,
  HIERARCHY_NAME           VARCHAR2(200)                          ,
  ORG_LAST_UPDATED_BY      NUMBER                                 ,
  ORG_LAST_UPDATE_DATE     DATE                                   ,
  ORG_BRAND                VARCHAR2(150)                          ,
  ORG_LEVEL_NUM            NUMBER            DEFAULT -1           NOT NULL,
  ORG_LEVEL                VARCHAR2(30)      DEFAULT 'UNKNOWN'    NOT NULL,
  ORGANIZATION_CODE        VARCHAR2(30)                           ,
  WAG_PARENT_COMPANY_CODE  VARCHAR2(30)                           ,
  WAG_ORGANIZATION_CODE    VARCHAR2(30)                           ,
  WAG_REGION_CODE          VARCHAR2(30)                           ,
  WAG_SR_DIV_CODE          VARCHAR2(30)                           ,
  WAG_DIST_AREA_CODE       VARCHAR2(30)                           ,
  WAG_AREA_DIST_CODE       VARCHAR2(30)                           ,
  WAG_STORE_DEPT_CODE      VARCHAR2(30)                           ,
  HIER_LAST_UPDATED_BY     NUMBER                                 ,
  HIER_LAST_UPDATE_DATE    DATE                                   ,
  PARENT_ORG_ID            NUMBER(15)                             ,
  PARENT_ORG_NAME          VARCHAR2(240)                          ,
  REQUEST_ID               NUMBER            DEFAULT -1           ,
  STATUS_CODE              VARCHAR2(1)       DEFAULT 'I'          NOT NULL );


ALTER TABLE XXARG.XXARG_HR_ORG_FLAT_HIERARCHY MODIFY
( CREATED_BY               NUMBER            DEFAULT -1           NOT NULL,
  CREATION_DATE            DATE              DEFAULT SYSDATE      NOT NULL );
  
  

DROP INDEX XXARG.XXARG_HR_ORG_FLAT_HIERARCHY_N8;

CREATE INDEX XXARG.XXARG_HR_ORG_FLAT_HIERARCHY_N8
  ON XXARG.XXARG_HR_ORG_FLAT_HIERARCHY (REQUEST_ID);



  
  
