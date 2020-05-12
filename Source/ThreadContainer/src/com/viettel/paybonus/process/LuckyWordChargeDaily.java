/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbLuckyWord;
import com.viettel.paybonus.obj.LuckyWordSub;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class LuckyWordChargeDaily extends ProcessRecordAbstract {

    public LuckyWordChargeDaily() {
        super();
        logger = Logger.getLogger(LuckyWordChargeDaily.class);
    }
    Exchange pro;
    DbLuckyWord db;
    String lwMsgPlayNotMovitelPreSub;
    String lwMsgPlayNotEnoughMoney;
    String lwMsgSytemFail;
    String lwMsgPlaysuccessNotWin;
    String lwMsgPlaysuccessWin;
    String lwMsgPlaysuccessWinToChiefVas;
    String lwWinWord;
    String[] lwListWinLetter;
    int lwMaxWinner;
    String lwLetter;
    String[] lwListLetter;
    String lwChiefVas;
    String[] lwListChiefVas;
    String lwPlayMoneyFee;
    String lwBonusWinner;
    ArrayList<String> listErrNotEnoughBalance;
    String[] lwListLetterNotWin;
    String lwNotWinWord;

    @Override
    public void initBeforeStart() throws Exception {
        logger.info("Initing LuckyWordPlay process before start ");
        db = new DbLuckyWord("dbluckyword", logger);
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        lwMsgPlayNotMovitelPreSub = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlayNotMovitelPreSub");
        lwMsgPlayNotEnoughMoney = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlayNotEnoughMoney");
        lwMsgSytemFail = ResourceBundle.getBundle("configPayBonus").getString("lwMsgSytemFail");
        lwMsgPlaysuccessWin = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlaysuccessWin");
        lwMsgPlaysuccessNotWin = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlaysuccessNotWin");
        lwMsgPlaysuccessWinToChiefVas = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlaysuccessWinToChiefVas");
//        lwWinWord = ResourceBundle.getBundle("configPayBonus").getString("lwWinWord");
        //LinhNBV start modified on September 21 2017: Get WinWord and reset WinWord if DAY_OF_WEEK is Monday
        lwWinWord = db.getCurrentLuckyWinWord();
        //This case for getWinWord fail
        if (lwWinWord == null || lwWinWord.trim().length() <= 0) {
            lwWinWord = "M|O|V|I|T|E|L";
        }
        lwLetter = ResourceBundle.getBundle("configPayBonus").getString("lwLetter");
//        lwMaxWinner = Integer.valueOf(ResourceBundle.getBundle("configPayBonus").getString("lwMaxWinner"));
        lwMaxWinner = db.getMaxWinner();
        //LinhNBV end.
        lwListLetter = lwLetter.split("\\|");
        lwListWinLetter = lwWinWord.split("\\|");
        lwChiefVas = ResourceBundle.getBundle("configPayBonus").getString("lwChiefVas");
        lwListChiefVas = lwChiefVas.split("\\|");
        lwPlayMoneyFee = ResourceBundle.getBundle("configPayBonus").getString("lwPlayMoneyFee");
        lwBonusWinner = ResourceBundle.getBundle("configPayBonus").getString("lwBonusWinner");
        listErrNotEnoughBalance = new ArrayList<String>(Arrays.asList(ResourceBundle.getBundle("configPayBonus").getString("ERR_BALANCE_NOT_ENOUGH").split("\\|")));
        lwNotWinWord = ResourceBundle.getBundle("configPayBonus").getString("lwLetter");
        for (String letter : lwWinWord.split("\\|")) {
            if (lwNotWinWord.contains(letter)) {
                lwNotWinWord = lwNotWinWord.replace(letter + "|", "");
            }
        }
        lwListLetterNotWin = lwNotWinWord.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            LuckyWordSub moRecord = (LuckyWordSub) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        String resultChargeMoney;
        int result;
        String word;
        String letter;
        int numberWinner;
        int numberWinnerInDay;
        boolean resetWeek = false;
        for (Record record : listRecord) {
            result = 0;
            numberWinner = 0;
            numberWinnerInDay = 0;
            resultChargeMoney = "";
            word = "";
            letter = "";
            LuckyWordSub moRecord = (LuckyWordSub) record;
//            Step 1: Check monday to reset data of old week
            Calendar cal = Calendar.getInstance();
            cal.setTime(new Date());
            if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) {
                if (!resetWeek) {
                    logger.info("New week so must reset data for old week");
                    db.resetOldWeek();
                    logger.info("New week so must get set new WinWord and new listWinLetter");
                    db.updateCurrentLuckyWinWord();
                    lwWinWord = db.getCurrentLuckyWinWord();
                    if (lwWinWord == null || lwWinWord.trim().length() <= 0) {
                        lwWinWord = "M|O|V|I|T|E|L";
                    }
                    lwListWinLetter = lwWinWord.split("\\|");
                    for (String newLetter : lwWinWord.split("\\|")) {
                        if (lwNotWinWord.contains(newLetter)) {
                            lwNotWinWord = lwNotWinWord.replace(newLetter + "|", "");
                        }
                    }
                    lwListLetterNotWin = lwNotWinWord.split("\\|");
                    resetWeek = true;
                }
            }
//            Step 1: Validate A is Movitel subscriber
            if (!db.checkPreSub(moRecord.getMsisdn().substring(3))) {
                logger.warn("The sub is not Pre Movitel subscriber " + moRecord.getMsisdn());
                moRecord.setResultCode("E01");
                moRecord.setDescription("Not Prepaid Movitel subscriber");
                continue;
            }
//            Step 2: Charge money
            resultChargeMoney = pro.addMoney(moRecord.getMsisdn(), "-" + lwPlayMoneyFee, "2000");
            if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
                logger.warn("Not enough money to to get auto letter " + moRecord.getMsisdn() + " errcode chargemoney " + resultChargeMoney);
                moRecord.setResultCode("E02");
                moRecord.setDescription("Not enough money to get auto letter");
                continue;
            } else if (!"0".equals(resultChargeMoney)) {
                logger.warn("Fail to charge money for getting auto letter " + moRecord.getMsisdn() + " errcode chargemoney " + resultChargeMoney);
                moRecord.setResultCode("E03");
                moRecord.setDescription("Fail to charge money for getting auto letter");
                continue;
            }
            numberWinner = db.countWinner(moRecord.getMsisdn());
            if (numberWinner < 0) {
                logger.warn("Fail to count winner, sub " + moRecord.getMsisdn() + " , now rollback money");
                moRecord.setResultCode("E04");
                moRecord.setDescription("Fail to count winner for getting auto letter, now rollback money");
                pro.addMoney(moRecord.getMsisdn(), lwPlayMoneyFee, "2000");
                continue;
            }
            moRecord.setMoney(lwPlayMoneyFee);
//          Step3: get current word, genarate letter
            word = db.viewWord(moRecord.getMsisdn());
//            Step 4: Check enough winner
//            Step 4.1: Check Winner in day.
            numberWinnerInDay = db.countWinnerInDay(moRecord.getMsisdn());
            if (numberWinnerInDay < 0) {
                logger.warn("Fail to count winner in day, sub " + moRecord.getMsisdn() + " , now rollback money");
                moRecord.setResultCode("E06");
                moRecord.setDescription("Fail to count winner in day for getting auto letter, now rollback money");
                pro.addMoney(moRecord.getMsisdn(), lwPlayMoneyFee, "2000");
                continue;
            }
            int maxWinnerInDay = lwMaxWinner / 7;
            logger.info("Total winner in week: " + lwMaxWinner + "\nNumber winner in day: " + numberWinnerInDay
                    + "\nMax winner in day: " + maxWinnerInDay + " " + moRecord.getMsisdn());
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                maxWinnerInDay = lwMaxWinner - (lwMaxWinner / 7) * 6; //So giai thuong con lai cho ngay Chu nhat
                logger.info("Max winner in day for Sunday: " + maxWinnerInDay);
            }
            if (numberWinner >= lwMaxWinner || numberWinnerInDay >= maxWinnerInDay) {
                if (numberWinner >= lwMaxWinner) {
                    logger.info("Aready enough winner, so now must limit to not have winner more " + moRecord.getMsisdn());
                } else {
                    logger.info("Aready enough winner today, so now must limit to not have winner more in today " + moRecord.getMsisdn());
                }
                Random generator = new Random();
                int i = generator.nextInt(lwListLetterNotWin.length);
                if (i < 0) {
                    i = 0;
                }
                if (i > lwListLetterNotWin.length - 1) {
                    i = lwListLetterNotWin.length - 1;
                }
                letter = lwListLetterNotWin[i];
                if (word != null && word.trim().length() > 0) {
                    logger.info("Old word " + word + " msisdn " + moRecord.getMsisdn());
                    word = word + letter;
                    logger.info("New word " + word + " msisdn " + moRecord.getMsisdn());
                } else {
                    word = letter;
                    logger.info("The first time pay, new word " + word + " msisdn " + moRecord.getMsisdn());
                }
            } else {
                Random generator = new Random();
                int i = generator.nextInt(lwListLetter.length);
                if (i < 0) {
                    i = 0;
                }
                if (i > lwListLetter.length - 1) {
                    i = lwListLetter.length - 1;
                }
                letter = lwListLetter[i];
                if (word != null && word.trim().length() > 0) {
                    logger.info("Old word " + word + " msisdn " + moRecord.getMsisdn());
                    word = word + letter;
                    logger.info("New word " + word + " msisdn " + moRecord.getMsisdn());
                } else {
                    word = letter;
                    logger.info("The first time pay in this week, new word " + word + " msisdn " + moRecord.getMsisdn());
                }
            }
            int win = 0;
            int countInWinWord = 0;
            int countInWord = 0;
            for (String s : lwListWinLetter) {
                countInWord = 0;
                countInWinWord = 0;
                for (int j = 0; j < word.length(); j++) {
                    if (s.charAt(0) == word.charAt(j)) {
                        countInWord++;
                    }
                }
                for (int k = 0; k < lwWinWord.length(); k++) {
                    if (s.charAt(0) == lwWinWord.charAt(k)) {
                        countInWinWord++;
                    }
                }
                if (countInWord >= countInWinWord) {
                    logger.info("count in word greater than " + countInWord + " count in win word "
                            + countInWinWord + " " + moRecord.getMsisdn() + " so increate win");
                    win += 1;
                }
            }
//            ArrayList<String> listDuplicateLetter = new ArrayList<String>();
//            for (int k = 0; k < lwListWinLetter.length; k++) {
//                if (word.contains(lwListWinLetter[k].trim().toUpperCase())
//                        && !listDuplicateLetter.contains(lwListWinLetter[k].trim().toUpperCase())) {
//                    listDuplicateLetter.add(lwListWinLetter[k].trim().toUpperCase());
//                    win = win + 1;
//                }
//            }
            if (win >= lwListWinLetter.length) {
                logger.info("WINNER word " + word + " letter " + letter + " msisdn " + moRecord.getMsisdn());
                result = db.saveDataPlay(0, moRecord.getMsisdn(), moRecord.getId(), letter, word, "1");
                db.clearDataWhenWin(moRecord.getMsisdn().trim(), moRecord.getId());
                if (result <= 0) {
                    logger.warn("Fail to save DataPlay sub " + moRecord.getMsisdn() + " result " + result + ", now rollback money");
                    moRecord.setResultCode("E05");
                    moRecord.setDescription("Fail to save DataPlay for getting auto letter, now rollback money");
                    pro.addMoney(moRecord.getMsisdn(), lwPlayMoneyFee, "2000");
                    continue;
                }
                //LinhNBV start modified on August 26 2017: Get Total sub and players
                //Total sub: (%ON%)/(%WIN%)\nPlayers: (%ONPLAYER%)/(%WINPLAYER%).
                int totalSubOn = db.getTotalSub("ON");
                int totalSubWin = db.getTotalSub("WIN");
                int totalPlayOn = db.getTotalPlayer("ON");
                int totalPlayWin = db.getTotalPlayer("WIN");
                int totalSubOff = db.getTotalSubOff();


                lwMsgPlaysuccessWinToChiefVas = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlaysuccessWinToChiefVas");
                if (lwMsgPlaysuccessWinToChiefVas != null && lwMsgPlaysuccessWinToChiefVas.trim().length() > 0) {
                    logger.info("WINNER notify Chief of Vas Dept " + " msisdn " + moRecord.getMsisdn() + " ListChiefVas " + lwChiefVas);
                    lwMsgPlaysuccessWinToChiefVas = lwMsgPlaysuccessWinToChiefVas.replace("%WINNER%", moRecord.getMsisdn());
                    lwMsgPlaysuccessWinToChiefVas = lwMsgPlaysuccessWinToChiefVas.replace("%COUNT%", (numberWinner + 1) + "");
                    lwMsgPlaysuccessWinToChiefVas = lwMsgPlaysuccessWinToChiefVas.replace("%ON%", String.valueOf(totalSubOn));
                    lwMsgPlaysuccessWinToChiefVas = lwMsgPlaysuccessWinToChiefVas.replace("%WIN%", String.valueOf(totalSubWin));
                    lwMsgPlaysuccessWinToChiefVas = lwMsgPlaysuccessWinToChiefVas.replace("%ONPLAYER%", String.valueOf(totalPlayOn));
                    lwMsgPlaysuccessWinToChiefVas = lwMsgPlaysuccessWinToChiefVas.replace("%WINPLAYER%", String.valueOf(totalPlayWin));
                    lwMsgPlaysuccessWinToChiefVas = lwMsgPlaysuccessWinToChiefVas.replace("%OFF%", String.valueOf(totalSubOff));
                    for (String s : lwListChiefVas) {
                        db.sendSms(s, lwMsgPlaysuccessWinToChiefVas, "1567");
                    }
                }
                lwMsgPlaysuccessWin = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlaysuccessWin");
                lwMsgPlaysuccessWin = lwMsgPlaysuccessWin.replace("%WORD%", word);
                moRecord.setResultCode("0");
                moRecord.setDescription("WINNER word " + word + " letter " + letter + " msisdn " + moRecord.getMsisdn());
//                Send sms to winner
                db.sendSms(moRecord.getMsisdn(), lwMsgPlaysuccessWin, "1567");
//                Add bonus for winner
                pro.topupPrePaid(moRecord.getMsisdn(), lwBonusWinner);
                continue;
            }
            logger.info("Not yet win current word " + word + " new letter " + letter + " msisdn " + moRecord.getMsisdn());
            result = db.saveDataPlay(0, moRecord.getMsisdn(), moRecord.getId(), letter, word, "0");
            if (result <= 0) {
                logger.warn("Fail to save DataPlay sub " + moRecord.getMsisdn() + " result " + result + ", now rollback money");
                moRecord.setResultCode("0");
                moRecord.setDescription("Fail to save DataPlay for getting auto letter, now rollback money");
                pro.addMoney(moRecord.getMsisdn(), lwPlayMoneyFee, "2000");
                continue;
            }
            lwMsgPlaysuccessNotWin = ResourceBundle.getBundle("configPayBonus").getString("lwMsgPlaysuccessNotWin");
            lwMsgPlaysuccessNotWin = lwMsgPlaysuccessNotWin.replace("%LETTER%", letter);
            lwMsgPlaysuccessNotWin = lwMsgPlaysuccessNotWin.replace("%WORD%", word);
            lwMsgPlaysuccessNotWin = lwMsgPlaysuccessNotWin.replace("%lwWinWord%", lwWinWord.replace("|", ""));
            moRecord.setResultCode("0");
            moRecord.setDescription("Auto get lettter success but not win");
//                Send sms to winner
            db.sendSms(moRecord.getMsisdn(), lwMsgPlaysuccessNotWin, "1567");
            continue;
        }
        Thread.sleep(60000);
        return listRecord;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tluckyword_charge_id|").
                append("|\tluckyword_sub_id\t|").
                append("|\tmsisdn\t|");
        for (Record record : listRecord) {
            LuckyWordSub bn = (LuckyWordSub) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getLuckyWordSubId()).
                    append("||\t").
                    append(bn.getMsisdn());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
//        logger.warn("TEMPLATE process exception record: " + ex.toString());
//        for (Record record : listRecord) {
//            logger.info("TEMPLATE let convert to recort type you want and then set errCode, errDesc at here");
////            MoRecord moRecord = (MoRecord) record;
////            moRecord.setMessage("Thao tac that bai!");
////            moRecord.setErrCode("-5");
//        }
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }

    public static void main(String[] args) {
        BigInteger dataValue = new BigInteger("10240");
        BigInteger dataHeigh = new BigInteger("1048576");
        dataValue = dataValue.multiply(dataHeigh);
        System.out.println("chia " + (20 / 7));
        System.out.println(String.valueOf(dataValue));
        System.out.println("doan dau" + "  thoi test  ".trim());
    }
}
