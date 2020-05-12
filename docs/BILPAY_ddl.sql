-- Start of DDL Script for Table CM_POS.BILPAY_SUB_INFO
-- Generated 5/8/2020 10:09:20 PM from CM_POS@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.229.41.121)(PORT = 9988)))(CONNECT_DATA =(SERVICE_NAME = DBTEMP)))

CREATE TABLE bilpay_sub_info
    (sub_id                         NUMBER(10,0) NOT NULL,
    isdn                           NUMBER(10,0) NOT NULL,
    serial                         VARCHAR2(20 BYTE) NOT NULL,
    contract_id                    NUMBER(10,0) NOT NULL,
    cus_id                         NUMBER(10,0) NOT NULL,
    create_time                    DATE DEFAULT sysdate,
    create_staff                   VARCHAR2(50 BYTE) NOT NULL,
    create_shop                    VARCHAR2(50 BYTE) NOT NULL,
    end_datetime                   DATE,
    sta_datetime                   DATE NOT NULL,
    act_status                     VARCHAR2(3 BYTE) NOT NULL,
    status                         NUMBER(1,0) ,
    deposit                        NUMBER(12,2),
    limit                          NUMBER(12,2),
    product_code                   VARCHAR2(30 BYTE) NOT NULL,
    telecom_service_id             NUMBER(10,0) NOT NULL)
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  TABLESPACE  data
  STORAGE   (
    INITIAL     3145728
    NEXT        1048576
    MINEXTENTS  1
    MAXEXTENTS  2147483645
  )
  NOCACHE
  MONITORING
  NOPARALLEL
  NOLOGGING
/





-- Indexes for BILPAY_SUB_INFO

CREATE INDEX bsi_isdn_idx ON bilpay_sub_info
  (
    isdn                            ASC
  )
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  TABLESPACE  data
NOPARALLEL
LOGGING
/



-- Constraints for BILPAY_SUB_INFO

ALTER TABLE bilpay_sub_info
ADD CONSTRAINT bsi_pk PRIMARY KEY (sub_id)
USING INDEX
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  TABLESPACE  data
/


-- End of DDL Script for Table CM_POS.BILPAY_SUB_INFO


-- Start of DDL Script for Table CM_POS.BILPAY_SUB_CHARGE
-- Generated 5/8/2020 10:08:54 PM from CM_POS@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.229.41.121)(PORT = 9988)))(CONNECT_DATA =(SERVICE_NAME = DBTEMP)))

CREATE TABLE bilpay_sub_charge
    (sub_charge_id                  NUMBER(10,0) NOT NULL,
    sub_id                         NUMBER(10,0) NOT NULL,
    isdn                           VARCHAR2(30 BYTE) NOT NULL,
    contract_id                    NUMBER(10,0) NOT NULL,
    cus_id                         NUMBER(10,0) NOT NULL,
    product_code                   VARCHAR2(50 BYTE) NOT NULL,
    tel_service_id                 NUMBER(2,0) NOT NULL,
    bill_cycle                     DATE DEFAULT trunc(sysdate, 'mm'),
    charge_time                    DATE DEFAULT sysdate,
    charge_value                   NUMBER(15,4) DEFAULT 0 NOT NULL,
    item_id                        NUMBER(10,0) NOT NULL,
    thread_name                    VARCHAR2(50 BYTE) NOT NULL)
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  TABLESPACE  data
  STORAGE   (
    BUFFER_POOL DEFAULT
  )
  NOCACHE
  MONITORING
  PARTITION BY RANGE (BILL_CYCLE)
  (
  PARTITION start01 VALUES LESS THAN (TO_DATE(' 2020-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  LOGGING
  )
  NOPARALLEL
/





-- Indexes for BILPAY_SUB_CHARGE

CREATE INDEX bsc_isdn_idx ON bilpay_sub_charge
  (
    bill_cycle                      ASC,
    isdn                            ASC
  )
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  STORAGE   (
    BUFFER_POOL DEFAULT
  )
NOPARALLEL
  LOCAL (
  PARTITION START01
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  LOGGING
  )
/



-- Constraints for BILPAY_SUB_CHARGE

ALTER TABLE bilpay_sub_charge
ADD CONSTRAINT bsc_pk PRIMARY KEY (sub_charge_id)
USING INDEX
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  TABLESPACE  data
/


-- End of DDL Script for Table CM_POS.BILPAY_SUB_CHARGE

-- Start of DDL Script for Table CM_POS.BILPAY_SUB_DEBIT
-- Generated 5/8/2020 10:08:55 PM from CM_POS@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.229.41.121)(PORT = 9988)))(CONNECT_DATA =(SERVICE_NAME = DBTEMP)))

CREATE TABLE bilpay_sub_debit
    (sub_debit_id                   NUMBER(10,0) NOT NULL,
    sub_id                         NUMBER(10,0) NOT NULL,
    isdn                           VARCHAR2(30 BYTE) NOT NULL,
    contract_id                    NUMBER(10,0) NOT NULL,
    cus_id                         NUMBER(10,0) NOT NULL,
    product_code                   VARCHAR2(50 BYTE) NOT NULL,
    tel_service_id                 NUMBER(2,0) NOT NULL,
    bill_cycle                     DATE DEFAULT trunc(sysdate, 'mm'),
    start_debit                    NUMBER(15,4) DEFAULT 0 NOT NULL,
    end_debit                      NUMBER(15,4) DEFAULT 0 NOT NULL,
    hot_charge                     NUMBER(15,4) DEFAULT 0 NOT NULL,
    remain_debit                   NUMBER(15,4) DEFAULT 0 NOT NULL,
    pay_value                      NUMBER(10,0) DEFAULT 0 NOT NULL,
    remain_money                   NUMBER(15,4) DEFAULT 0 NOT NULL,
    adjust_value                   NUMBER(10,0) NOT NULL,
    bad_debit                      NUMBER(15,4) DEFAULT 0 NOT NULL,
    last_charge_time               DATE DEFAULT sysdate,
    last_charge_id                 NUMBER(10,0) NOT NULL,
    last_pay_time                  DATE DEFAULT sysdate,
    last_pay_id                    NUMBER(10,0) NOT NULL)
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  TABLESPACE  data
  STORAGE   (
    BUFFER_POOL DEFAULT
  )
  NOCACHE
  MONITORING
  PARTITION BY RANGE (BILL_CYCLE)
  (
  PARTITION start01 VALUES LESS THAN (TO_DATE(' 2020-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  LOGGING
  )
  NOPARALLEL
/





-- Indexes for BILPAY_SUB_DEBIT

CREATE INDEX bsd_isdn_idx ON bilpay_sub_debit
  (
    bill_cycle                      ASC,
    isdn                            ASC
  )
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  STORAGE   (
    BUFFER_POOL DEFAULT
  )
NOPARALLEL
  LOCAL (
  PARTITION START01
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  LOGGING
  )
/



-- Constraints for BILPAY_SUB_DEBIT

ALTER TABLE bilpay_sub_debit
ADD CONSTRAINT bsd_pk PRIMARY KEY (sub_debit_id)
USING INDEX
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  TABLESPACE  data
/


-- Comments for BILPAY_SUB_DEBIT

COMMENT ON COLUMN bilpay_sub_debit.adjust_value IS 'tong tien dieu chinh trong ky'
/
COMMENT ON COLUMN bilpay_sub_debit.bad_debit IS 'tong no xau kho doi'
/
COMMENT ON COLUMN bilpay_sub_debit.end_debit IS 'no cuoi ky = 0 khi giao thu, chot cuoc thi cap nhat = remain_debit cua ky duoc chot'
/
COMMENT ON COLUMN bilpay_sub_debit.hot_charge IS 'cuoc nong phat sinh trong ky'
/
COMMENT ON COLUMN bilpay_sub_debit.pay_value IS 'ton tien da thanh toan trong ky'
/
COMMENT ON COLUMN bilpay_sub_debit.remain_debit IS 'no con phai thu trong ky'
/
COMMENT ON COLUMN bilpay_sub_debit.remain_money IS 'tien thua trong ky'
/
COMMENT ON COLUMN bilpay_sub_debit.start_debit IS 'no dau ky'
/

-- End of DDL Script for Table CM_POS.BILPAY_SUB_DEBIT

-- Start of DDL Script for Table CM_POS.BILPAY_SUB_PAYMENT
-- Generated 5/8/2020 10:08:57 PM from CM_POS@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.229.41.121)(PORT = 9988)))(CONNECT_DATA =(SERVICE_NAME = DBTEMP)))

CREATE TABLE bilpay_sub_payment
    (sub_payment_id                 NUMBER(10,0) NOT NULL,
    pay_trans_id                   NUMBER(10,0) NOT NULL,
    sub_id                         NUMBER(10,0) NOT NULL,
    isdn                           VARCHAR2(30 BYTE) NOT NULL,
    contract_id                    NUMBER(10,0) NOT NULL,
    cus_id                         NUMBER(10,0) NOT NULL,
    product_code                   VARCHAR2(50 BYTE) NOT NULL,
    tel_service_id                 NUMBER(2,0) NOT NULL,
    pay_time                       DATE DEFAULT sysdate,
    total_pay_val                  NUMBER(15,4) DEFAULT 0 NOT NULL,
    sub_pay_val                    NUMBER(15,4) DEFAULT 0,
    pay_method                     NUMBER(2,0) DEFAULT 0 NOT NULL,
    sub_debit_id                   NUMBER(10,0) NOT NULL,
    bill_cycle                     DATE NOT NULL,
    remain_debit_before            NUMBER(15,4) DEFAULT 0,
    remain_debit_after             NUMBER(15,4) DEFAULT 0,
    bank_code1                     VARCHAR2(20 BYTE) DEFAULT 0,
    bank_code2                     VARCHAR2(20 BYTE) DEFAULT 0,
    bank_code3                     VARCHAR2(20 BYTE),
    emola_trans_id                 VARCHAR2(20 BYTE) DEFAULT 0,
    pagamento_trans_id             VARCHAR2(20 BYTE) DEFAULT NULL,
    result_code                    VARCHAR2(10 BYTE) NOT NULL,
    result_desc                    VARCHAR2(100 BYTE) DEFAULT NULL NOT NULL,
    create_user                    VARCHAR2(100 BYTE) NOT NULL)
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  TABLESPACE  data
  STORAGE   (
    BUFFER_POOL DEFAULT
  )
  NOCACHE
  MONITORING
  PARTITION BY RANGE (PAY_TIME)
  (
  PARTITION start01 VALUES LESS THAN (TO_DATE(' 2020-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  LOGGING
  )
  NOPARALLEL
/





-- Indexes for BILPAY_SUB_PAYMENT

CREATE INDEX bsp_isdn_idx ON bilpay_sub_payment
  (
    pay_time                        ASC,
    isdn                            ASC
  )
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  STORAGE   (
    BUFFER_POOL DEFAULT
  )
NOPARALLEL
  LOCAL (
  PARTITION START01
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  LOGGING
  )
/



-- Constraints for BILPAY_SUB_PAYMENT

ALTER TABLE bilpay_sub_payment
ADD CONSTRAINT bsp_pk PRIMARY KEY (sub_payment_id)
USING INDEX
  PCTFREE     10
  INITRANS    2
  MAXTRANS    255
  TABLESPACE  data
/


-- Comments for BILPAY_SUB_PAYMENT

COMMENT ON COLUMN bilpay_sub_payment.create_user IS 'user lam giao dich thanh toan'
/
COMMENT ON COLUMN bilpay_sub_payment.pagamento_trans_id IS 'la bank_file_detail_id trong bang bank_file_his'
/
COMMENT ON COLUMN bilpay_sub_payment.sub_debit_id IS 'ban ghi no cuoc duoc gach'
/
COMMENT ON COLUMN bilpay_sub_payment.sub_pay_val IS 'so tien thanh toan cho thue bao'
/
COMMENT ON COLUMN bilpay_sub_payment.total_pay_val IS 'tong tien thanh toan'
/

-- End of DDL Script for Table CM_POS.BILPAY_SUB_PAYMENT


CREATE SEQUENCE bilpay_sub_payment_seq
  INCREMENT BY 1
  START WITH 1
  MINVALUE 1
  MAXVALUE 999999999999999999999999999
  NOCYCLE
  NOORDER
  NOCACHE
/

CREATE SEQUENCE bilpay_sub_charge_seq
  INCREMENT BY 1
  START WITH 1
  MINVALUE 1
  MAXVALUE 999999999999999999999999999
  NOCYCLE
  NOORDER
  NOCACHE
/

CREATE SEQUENCE bilpay_sub_debit_seq
  INCREMENT BY 1
  START WITH 1
  MINVALUE 1
  MAXVALUE 999999999999999999999999999
  NOCYCLE
  NOORDER
  NOCACHE
/



