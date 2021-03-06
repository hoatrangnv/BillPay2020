/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.common;

/**
 * Cấu hình chung
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 28, 2012
 */
public class Vas {

	public class Mdm {

		public static final String SUCCESS = "0";
		public static final String NOT_SUPPORT = "NOTSUPPORT";
	}

	public class Provisioning {

		public static final int CENT_UNITS = 10000; // chia tai khoan tien cho so nay de ra tai khoan thuc bang cent
		public static final int USD_UNITS = 1000000; // chia tai khoan tien cho so nay de ra tai khoan thuc bang $
		public static final String BASIC_BALANCE = "2000"; // ma tai khoan goc 1
		public static final String PROMOTION_BALANCE = "2001"; // ma tai khoan khuyen mai
		public static final String PROMOTION_2_BALANCE = "2100"; // ma tai khoan khuyen mai(Promotion account onlyfor OnNet call - chi dung cho Call)
		public static final String CALL_FREE_BALANCE = "4000"; // ma tai khoan call khuyen mai noi mang
		public static final String CALL_FREE_BALANCE2 = "4046"; // ma tai khoan call khuyen mai noi mang, ngoai mang
		public static final String SMS_FREE_BALANCE = "4200"; // ma tai khoan sms khuyen mai noi mang
		public static final String DATA_FREE_BALANCE = "4561"; // ma tai khoan sms khuyen mai
//        public static final String CALL_INTERNATION_BALANCE = "null"; // ma tai khoan call nuoc ngoai
//        public static final String SMS_INTERNATION_BALANCE = "null"; // ma tai khoan sms nuoc ngoai
		//        

		public class AdjustAccount {

			public static final String PC_ADJUST_ACCOUNT = "pc_adjust_account"; // process_code lenh thay doi tai khoan
			public static final String PARAM_MSISDN = "param_adjust_account_msisdn"; // tham so msisdn cua lenh thay doi tai khoan
			public static final String PARAM_ACCOUNT_ID = "param_adjust_account_account_id"; // tham so msisdn cua lenh thay doi tai khoan
			public static final String PARAM_RESET_BALANCE = "param_adjust_account_reset_balance"; // tham so account_id cua lenh thay doi tai khoan
			public static final String PARAM_ADD_BALANCE = "param_adjust_account_add_balance"; // tham so account_id cua lenh thay doi tai khoan
			public static final String PARAM_EXPIRE_DATE = "param_adjust_account_expire_date"; // tham so account_id cua lenh thay doi tai khoan
		}

		public class ViewAccount {

			public static final String PC_VIEW_ACCOUNT = "pc_view_account"; // process_code lenh view tai khoan
			public static final String PARAM_MSISDN = "param_view_account_msisdn"; // tham so msisdn cua lenh view
		}
	}

	public class Constanst {

		public static final int POSTPAID = 0;
		public static final int PREPAID = 1;
		public static final int HP_POSTPAID = 2;
		public static final int HP_PREPAID = 3;
		public static final int FIX_BROADBAND = 4;
		public static final int FIX_BROADBAND_PREPAID = 5;
		public static final float BALANCE_ERROR = -999999;
		public static final int API_SALE_REVENUE_TYPE = 1;
		public static final int API_PAYMENT_REVENUE_TYPE = 2;
		public static final int VIPSUB = 6;
	}

	public class ActionCdr {

		public static final String REGISTER = "0";
		public static final String REMOVE = "1";
	}

	public class Module {

		public static final String RECEIVER = "RECEIVER";
		public static final String PROCESS = "PROCESS";
		public static final String PROVISIONING = "PROVISIONING";
	}

	public class Topup {

        public static final String SUB_TYPE_MOBILE_PRE = "1";
        public static final String SUB_TYPE_MOBILE_POS = "2";
        public static final String SUB_TYPE_FBB = "3";
        public static final String TRANS_TYPE_TOPUP = "1";
        public static final String TRANS_TYPE_RECHARGE = "2";
        public static final String TRANS_TYPE_BUY_MI = "3";
        public static final String TRANS_TYPE_TOPUP_HAVE_PRE_FUND = "4";
        public static final String TRANS_TYPE_TOPUP_HAVE_POS_FUND = "5";
        public static final String INPUT_ERROR = "44";
        public static final String EXCEPTION = "23";
        public static final String INVALID_CHARGING_AMOUNT = "36";
        public static final String DATABASE_ERROR = "88";
        public static final String SUCCESSFUL = "0";
        public static final String FAIL_RECHARGE = "63";
        public static final String NOT_EXISTS = "77";
        public static final String FUND_INVALID = "76";
        public static final String SIGNATURE_INVALID = "78";
        public static final String FAIL_PAID_FUND = "79";
        public static final String FAIL_SAME_REQUESTID = "80";
        public static final String FAIL_HAVE_DEBIT = "81";
    }

	public class ResultCode {

		public static final String SUCCESS = "0";
		public static final String ERROR = "ERROR";
		public static final long USER_NOT_FOUND = -1;
		public static final long IP_NOT_ALLOW = -2;
		public static final long WRONG_PASSWORD = -3;
		public static final int WRONG_ACCOUNT_IP = -4;
		public static final int INVALID_INPUT = 1;
		public static final int INVALID_CARD = 2;
		public static final int FAIL_GET_IP = 3;
		public static final int NOT_SUPPORT_POSTPAID = 4;
		public static final int NOT_ACTIVE = 5;
		public static final int INVALID_PRODUCT = 6;
		public static final int NOT_ENOUGH_MONEY = 7;
		public static final int ALREADY_HAVE_BOUNES = 8;
		public static final int FALSE_ADD_DATA = 9;
	}

	public class SyncBccsErpResultCode {

		//Common code
		public static final String API_SUCCESSFULLY_DECS = "Successfully";
		public static final String API_SUCCESSFULLY_CODE = "0";
		public static final String API_INVALID_USER_DESC = "Login failed invalid user";
		public static final String API_INVALID_USER_CODE = "01";
		public static final String API_EXCEPTION_OCCUR_DESC = "Have an occur while processing";
		public static final String API_EXCEPTION_OCCUR_CODE = "99";
		//Sync revenue
		public static final String API_PROCESSING_DECS = "System in processing.No data found";
		public static final String API_PROCESSING_CODE = "02";
		public static final String API_INVALID_REVENUE_TYPE_DESC = "Invalid revenue type";
		public static final String API_INVALID_REVENUE_TYPE_CODE = "03";
		public static final String API_INVALID_STAR_OF_CYCLE_DESC = "Invalid start of cycle";
		public static final String API_INVALID_STAR_OF_CYCLE_CODE = "04";
		public static final String API_INVALID_INPUT_DESC = "Invalid input";
		public static final String API_INVALID_INPUT_CODE = "05";
		public static final String FAIL_GET_IP_DESC = "Cannot get client IP";
		public static final String FAIL_GET_IP_CODE = "06";
		//Sync stock trans
		public static final String ERR_PARNER_CODE_EMPTY_MSS = "Partner code must be not null";
		public static final String ERR_PARNER_CODE_EMPTY = "11";
		public static final String ERR_PARNER_CODE_INVALID_MSS = "Partner code invalid";
		public static final String ERR_PARNER_CODE_INVALID = "12";
		public static final String ERR_PARNER_NAME_INVALID_MSS = "Partner name invalid";
		public static final String ERR_PARNER_NAME_INVALID = "13";
		public static final String ERR_PARNER_NAME_EMPTY = "14";
		public static final String ERR_PARNER_NAME_EMPTY_MSS = "Partner name must be not null";
		public static final String ERR_PARNER_TYPE_INVALID = "15";
		public static final String ERR_PARNER_TYPE_INVALID_MSS = "Partner type invalid";
		public static final String ERR_PARNER_TYPE_EMPTY = "16";
		public static final String ERR_PARNER_TYPE_EMPTY_MSS = "Partner type must be not null";
		public static final String ERR_PARNER_ADDRESS_INVALID = "17";
		public static final String ERR_PARNER_ADDRESS_INVALID_MSS = "Partner address invalid";
		public static final String ERR_PARNER_CONTACT_INVALID = "18";
		public static final String ERR_PARNER_CONTACT_INVALID_MSS = "Partner contact invalid";
		public static final String ERR_PARNER_PHONE_INVALID = "19";
		public static final String ERR_PARNER_PHONE_INVALID_SMS = "Partner phone invalid";
		public static final String ERR_PARNER_FAX_INVALID = "20";
		public static final String ERR_PARNER_FAX_INVALID_MSS = "Partner fax invalid";
		public static final String ERR_PARNER_EXISTED = "21";
		public static final String ERR_PARNER_EXISTED_MSS = "Partner code already existed";
		public static final String ERR_PARNER_NOT_EXIST = "22";
		public static final String ERR_PARNER_NOT_EXIST_MSS = "Partner dose not exist in BCCS";
		public static final String ERR_STOCK_MODEL_NOT_EXIST = "23";
		public static final String ERR_STOCK_MODEL_NOT_EXIST_MSS = "Stock model code does not exist in BCCS";
		public static final String ERR_STOCK_MODEL_CODE_EMPTY = "24";
		public static final String ERR_STOCK_MODEL_CODE_EMPTY_MSS = "Stock model code must be not null";
		public static final String ERR_STOCK_MODEL_INVALID = "25";
		public static final String ERR_STOCK_MODEL_INVALID_MSS = "Stock model code invalid";
		public static final String ERR_QUANTITY_EMPTY = "26";
		public static final String ERR_QUANTITY_EMPTY_MSS = "Quantity must be not null";
		public static final String ERR_QUANTITY_INVALID = "33";
		public static final String ERR_QUANTITY_INVALID_MSS = "Quantity must be integer";
		public static final String ERR_STATE_ID_EMPTY = "34";
		public static final String ERR_STATE_ID_EMPTY_MSS = "State id must be not null";
		public static final String ERR_STATE_ID_INVALID = "35";
		public static final String ERR_STATE_ID_INVALID_MSS = "State id invalid";
		public static final String ERR_CONTRACT_CODE_EMPTY = "27";
		public static final String ERR_CONTRACT_CODE_EMPTY_MSS = "Contact code must be not null";
		public static final String ERR_CONTRACT_CODE_INVALID = "28";
		public static final String ERR_CONTRACT_CODE_INVALID_MSS = "Contact code invalid";
		public static final String ERR_BATCH_CODE_EMPTY = "29";
		public static final String ERR_BATCH_CODE_EMPTY_MSS = "Batch code must be not null";
		public static final String ERR_BATCH_CODE_INVALID = "30";
		public static final String ERR_BATCH_CODE_INVALID_MSS = "Batch code invalid";
		public static final String ERR_REASON_ID_EMPTY = "31";
		public static final String ERR_REASON_ID_EMPTY_MSS = "Reason must be not null";
		public static final String ERR_REASON_ID_INVALID = "32";
		public static final String ERR_REASON_ID_INVALID_MSS = "Reason id invalid";
		public static final String ERR_REQUEST_ID_EMPTY = "35";
		public static final String ERR_REQUEST_ID_EMPTY_MSS = "Request code must be not null";
		public static final String ERR_REQUEST_ID_INVALID = "36";
		public static final String ERR_REQUEST_ID_INVALID_MSS = "Request id invalid";
		public static final String ERR_REQUEST_ID_EXISTED = "37";
		public static final String ERR_REQUEST_ID_EXISTED_MSS = "Request id already processed";
		public static final String ERR_NOTE_INVALID = "38";
		public static final String ERR_NOTE_INVALID_MSS = "Note info too long";
		//Sync banktransfer document
		public static final String ERR_DATE_FORMAT_MSS = "Input date incorrect format";
		public static final String ERR_DATE_FORMAT_CODE = "08";
		public static final String ERR_NO_DATA_CODE = "09";
		public static final String ERR_NO_DATA_MSS = "No data found";
	}
}
