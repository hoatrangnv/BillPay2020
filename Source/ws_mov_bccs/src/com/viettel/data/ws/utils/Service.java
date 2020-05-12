/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.data.ws.utils;

import com.viettel.common.ViettelService;
import com.viettel.vas.util.obj.ExchangeChannel;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.log4j.Logger;
import com.viettel.common.ViettelMsg;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.vas.wsfw.object.SubAdslLLPrepaid;

/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 28, 2012
 */
public class Service {

    private Logger logger;
    private String loggerLabel = Service.class.getSimpleName() + ": ";
    private ExchangeChannel channel;
    private StringBuffer br = new StringBuffer();
    private SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    public static final long REQUEST_TIME_OUT = 30000;

    public Service(ExchangeChannel channel, Logger logger) throws IOException {
        this.logger = logger;
        this.channel = channel;
//        this.dbProcessor = dbProcessor;
        try {
            logger.info(loggerLabel + "Connect Exchange Client-" + channel.getId());
        } catch (Exception ex) {
            logger.error(loggerLabel + "ERROR connect Exchange Client-" + channel.getId(), ex);
        }
    }

    public void logTime(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;
        StringBuffer br = new StringBuffer();
        if (timeEx >= AppManager.minTimeDb && AppManager.loggerDbMap != null) {
            br.setLength(0);
            br.append(loggerLabel).
                    append(AppManager.getTimeLevelDb(timeEx)).append(": ").
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");
            logger.warn(br);
        } else {
            br.setLength(0);
            br.append(loggerLabel).
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.info(br);
        }
    }

    public String activeFBB(SubAdslLLPrepaid sub, String center, String msisdn) {
        long timeSt = System.currentTimeMillis();
        ViettelMsg response = null;
        String err = "";
        try {
            if (msisdn.startsWith("258")) {
                msisdn = msisdn.substring(3);
            }
            logger.info("Start activeFBB for sub " + msisdn + " center " + center + " actStatus " + sub.getActStatus());
            ViettelService request = new ViettelService();
            request.setMessageType("1900");
            request.setProcessCode("240012");
            request.set("MSISDN", msisdn);
            request.set("ACT_STATUS", sub.getActStatus()); //use 100 KHYC because Rating not accept to open by this reason from other systems
            request.set("NUM_WAY", "1");
            request.set("ACTIVE_TYPE", "KHYC");
            request.set("USERNAME", msisdn);
            request.set("CENTER", center);
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MINUTE, 10); // to be greater the time on server
            request.set("EFFECT_DATE", sdf2.format(cal.getTime()));
            response = channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = (String) response.get("responseCode");
            if ("0".equals(err)) {
                logger.info("Success activeFBB isdn " + msisdn + " center " + center + " result " + err);
            } else {
                logger.info("Fail activeFBB isdn " + msisdn + " center " + center + " result " + err
                        + " request " + request + " response " + response);
            }
            return err;
        } catch (Exception ex) {
            logger.error("Had exception activeFBB isdn " + msisdn + " center " + center + " actStatus " + sub.getActStatus());
            logger.error(AppManager.logException(timeSt, ex));
            return "";
        }
    }
}
