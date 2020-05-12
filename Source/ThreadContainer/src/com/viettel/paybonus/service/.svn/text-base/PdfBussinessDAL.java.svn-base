package com.viettel.paybonus.service;

import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.FontFactory;
import com.itextpdf.text.Image;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.CMYKColor;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;
import com.viettel.paybonus.obj.RequestChannel;
import java.io.BufferedInputStream;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.net.ftp.FTPClient;

public class PdfBussinessDAL {

    private String path;
    private Document document;

    public PdfBussinessDAL(String path, Document document) {
        super();
        this.path = path;
        this.document = document;
    }

    public void createContractRegistrationPoint(RequestChannel contractInfo, String contractNumber, String sysdate,
            String pathFrontBI, String pathBackBI, String pathSignB, FTPClient ftpClient, String basePath) throws IOException {//sysdate: YYYYMMDD

        Font header = FontFactory.getFont(FontFactory.TIMES, 14, Font.BOLD,
                new CMYKColor(0, 0, 0, 255));
        Font normalFont = FontFactory.getFont(FontFactory.TIMES, 13, Font.NORMAL,
                new CMYKColor(0, 0, 0, 255));
        Font normalFontBold = FontFactory.getFont(FontFactory.TIMES, 13, Font.BOLD,
                new CMYKColor(0, 0, 0, 255));
        Font content = FontFactory.getFont(FontFactory.TIMES, 8, Font.NORMAL, BaseColor.BLACK);
        File myFile = new File(path);
        FileOutputStream output = null;
        try {
            output = new FileOutputStream(myFile);
            // Step 2
            PdfWriter pdfWriter = PdfWriter.getInstance(document, output);
            // Step 3
            document.open();
            // Step 4 Add content
            /* Hien thi tieu de */
            PdfPCell title = new PdfPCell(new Paragraph(ContractConstants.registration_form_1, header));
            title.setBorderColor(BaseColor.WHITE);
            title.setPaddingLeft(5);
            title.setHorizontalAlignment(Element.ALIGN_CENTER);
            title.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell contractNo = new PdfPCell(new Paragraph(String.format(ContractConstants.registration_contract_no, contractNumber), normalFont));
            contractNo.setBorderColor(BaseColor.WHITE);
            contractNo.setPaddingLeft(5);
            contractNo.setHorizontalAlignment(Element.ALIGN_CENTER);
            contractNo.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell dateTime = new PdfPCell(new Paragraph(String.format(ContractConstants.registration_date_time, sysdate.substring(6, 8), sysdate.substring(4, 6), sysdate.substring(0, 4)), header));
            dateTime.setBorderColor(BaseColor.WHITE);
            dateTime.setPaddingLeft(5);
            dateTime.setHorizontalAlignment(Element.ALIGN_CENTER);
            dateTime.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell tableA = new PdfPCell(new Paragraph(ContractConstants.registration_part_a, content));
            tableA.setBorderColor(BaseColor.BLACK);
            tableA.setPaddingLeft(10);
            tableA.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableA.setVerticalAlignment(Element.ALIGN_CENTER);
            tableA.setPaddingBottom(5);

            PdfPCell tableAContent = new PdfPCell(new Paragraph(ContractConstants.registration_movitel_sa, content));
            tableAContent.setBorderColor(BaseColor.BLACK);
            tableAContent.setPaddingLeft(10);
            tableAContent.setPaddingBottom(5);
            tableAContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableAContent.setVerticalAlignment(Element.ALIGN_CENTER);

            PdfPCell tableARepresentante = new PdfPCell(new Paragraph(ContractConstants.registration_representative, content));
            tableARepresentante.setBorderColor(BaseColor.BLACK);
            tableARepresentante.setPaddingLeft(10);
            tableARepresentante.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableARepresentante.setVerticalAlignment(Element.ALIGN_CENTER);
            tableARepresentante.setPaddingBottom(5);

            PdfPCell tableARepresentanteContent = new PdfPCell(new Paragraph(ContractConstants.registration_sr_nguyen_dat, content));
            tableARepresentanteContent.setBorderColor(BaseColor.BLACK);
            tableARepresentanteContent.setPaddingLeft(10);
            tableARepresentanteContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableARepresentanteContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableARepresentanteContent.setPaddingBottom(5);

            PdfPCell tableACargo = new PdfPCell(new Paragraph(ContractConstants.registration_position, content));
            tableACargo.setBorderColor(BaseColor.BLACK);
            tableACargo.setPaddingLeft(10);
            tableACargo.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableACargo.setVerticalAlignment(Element.ALIGN_CENTER);
            tableACargo.setPaddingBottom(5);

            PdfPCell tableACargoContent = new PdfPCell(new Paragraph(ContractConstants.registration_general_director, content));
            tableACargoContent.setBorderColor(BaseColor.BLACK);
            tableACargoContent.setPaddingLeft(10);
            tableACargoContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableACargoContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableACargoContent.setPaddingBottom(5);

            PdfPCell tableAEndereco = new PdfPCell(new Paragraph(ContractConstants.registration_address, content));
            tableAEndereco.setBorderColor(BaseColor.BLACK);
            tableAEndereco.setPaddingLeft(10);
            tableAEndereco.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableAEndereco.setVerticalAlignment(Element.ALIGN_CENTER);
            tableAEndereco.setPaddingBottom(5);

            PdfPCell tableAEnderecoContent = new PdfPCell(new Paragraph(ContractConstants.registration_address_part_a, content));
            tableAEnderecoContent.setBorderColor(BaseColor.BLACK);
            tableAEnderecoContent.setPaddingLeft(10);
            tableAEnderecoContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableAEnderecoContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableAEnderecoContent.setPaddingBottom(5);

            PdfPCell tableANuit = new PdfPCell(new Paragraph(ContractConstants.registration_nuit, content));
            tableANuit.setBorderColor(BaseColor.BLACK);
            tableANuit.setPaddingLeft(10);
            tableANuit.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableANuit.setVerticalAlignment(Element.ALIGN_CENTER);
            tableANuit.setPaddingBottom(5);

            PdfPCell tableANuitContent = new PdfPCell(new Paragraph(ContractConstants.registration_nuit_part_a, content));
            tableANuitContent.setBorderColor(BaseColor.BLACK);
            tableANuitContent.setPaddingLeft(10);
            tableANuitContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableANuitContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableANuitContent.setPaddingBottom(5);


            PdfPCell tableB = new PdfPCell(new Paragraph(ContractConstants.registration_part_b, content));
            tableB.setBorderColor(BaseColor.BLACK);
            tableB.setPaddingLeft(10);
            tableB.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableB.setVerticalAlignment(Element.ALIGN_CENTER);
            tableB.setPaddingBottom(5);

            PdfPCell tableBContent = new PdfPCell(new Paragraph(contractInfo.getChannelName(), content));
            tableBContent.setBorderColor(BaseColor.BLACK);
            tableBContent.setPaddingLeft(10);
            tableBContent.setPaddingBottom(5);
            tableBContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBContent.setVerticalAlignment(Element.ALIGN_CENTER);

            PdfPCell tableBRepresentante = new PdfPCell(new Paragraph(ContractConstants.registration_representative, content));
            tableBRepresentante.setBorderColor(BaseColor.BLACK);
            tableBRepresentante.setPaddingLeft(10);
            tableBRepresentante.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBRepresentante.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBRepresentante.setPaddingBottom(5);

            PdfPCell tableBRepresentanteContent = new PdfPCell(new Paragraph(contractInfo.getChannelName(), content));
            tableBRepresentanteContent.setBorderColor(BaseColor.BLACK);
            tableBRepresentanteContent.setPaddingLeft(10);
            tableBRepresentanteContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBRepresentanteContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBRepresentanteContent.setPaddingBottom(5);

            PdfPCell tableBBINumber = new PdfPCell(new Paragraph(ContractConstants.registration_bi_number, content));
            tableBBINumber.setBorderColor(BaseColor.BLACK);
            tableBBINumber.setPaddingLeft(10);
            tableBBINumber.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBBINumber.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBBINumber.setPaddingBottom(5);

            PdfPCell tableBBINumberContent = new PdfPCell(new Paragraph(contractInfo.getbINumber(), content));
            tableBBINumberContent.setBorderColor(BaseColor.BLACK);
            tableBBINumberContent.setPaddingLeft(10);
            tableBBINumberContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBBINumberContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBBINumberContent.setPaddingBottom(5);

            PdfPCell tableBValidoAte = new PdfPCell(new Paragraph(ContractConstants.registration_valid_until, content));
            tableBValidoAte.setBorderColor(BaseColor.BLACK);
            tableBValidoAte.setPaddingLeft(10);
            tableBValidoAte.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBValidoAte.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBValidoAte.setPaddingBottom(5);

            PdfPCell tableBValidoAteContent = new PdfPCell(new Paragraph("", content));//contractInfo.getValidUntil()
            tableBValidoAteContent.setBorderColor(BaseColor.BLACK);
            tableBValidoAteContent.setPaddingLeft(10);
            tableBValidoAteContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBValidoAteContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBValidoAteContent.setPaddingBottom(5);

            PdfPCell tableBEndereco = new PdfPCell(new Paragraph(ContractConstants.registration_address, content));
            tableBEndereco.setBorderColor(BaseColor.BLACK);
            tableBEndereco.setPaddingLeft(10);
            tableBEndereco.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBEndereco.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBEndereco.setPaddingBottom(5);

            PdfPCell tableBEnderecoContent = new PdfPCell(new Paragraph(contractInfo.getAddress() == null ? "" : contractInfo.getAddress(), content));//Address
            tableBEnderecoContent.setBorderColor(BaseColor.BLACK);
            tableBEnderecoContent.setPaddingLeft(10);
            tableBEnderecoContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBEnderecoContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBEnderecoContent.setPaddingBottom(5);


            PdfPCell tableBPhone = new PdfPCell(new Paragraph(ContractConstants.registration_phone_number, content));
            tableBPhone.setBorderColor(BaseColor.BLACK);
            tableBPhone.setPaddingLeft(10);
            tableBPhone.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBPhone.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBPhone.setPaddingBottom(5);

            PdfPCell tableBPhoneContent = new PdfPCell(new Paragraph(contractInfo.getChannelIsdn(), content));
            tableBPhoneContent.setBorderColor(BaseColor.BLACK);
            tableBPhoneContent.setPaddingLeft(10);
            tableBPhoneContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBPhoneContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBPhoneContent.setPaddingBottom(5);

            PdfPCell tableBPhoneEmola = new PdfPCell(new Paragraph(ContractConstants.registration_e_mola, content));
            tableBPhoneEmola.setBorderColor(BaseColor.BLACK);
            tableBPhoneEmola.setPaddingLeft(10);
            tableBPhoneEmola.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBPhoneEmola.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBPhoneEmola.setPaddingBottom(5);

            PdfPCell tableBPhoneEmolaContent = new PdfPCell(new Paragraph(contractInfo.getIsdnWallet(), content));
            tableBPhoneEmolaContent.setBorderColor(BaseColor.BLACK);
            tableBPhoneEmolaContent.setPaddingLeft(10);
            tableBPhoneEmolaContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableBPhoneEmolaContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableBPhoneEmolaContent.setPaddingBottom(5);

            PdfPCell contentAfterTableB = new PdfPCell(new Paragraph(ContractConstants.registration_form_2, content));
            contentAfterTableB.setBorderColor(BaseColor.WHITE);
            contentAfterTableB.setPaddingLeft(5);
            contentAfterTableB.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            contentAfterTableB.setVerticalAlignment(Element.ALIGN_MIDDLE);


            PdfPCell artigo1 = new PdfPCell(new Paragraph(ContractConstants.registration_form_3, normalFontBold));
            artigo1.setBorderColor(BaseColor.WHITE);
            artigo1.setPaddingLeft(5);
            artigo1.setHorizontalAlignment(Element.ALIGN_CENTER);
            artigo1.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo11 = new PdfPCell(new Paragraph(ContractConstants.registration_form_4, content));
            artigo11.setBorderColor(BaseColor.WHITE);
            artigo11.setPaddingLeft(5);
            artigo11.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo11.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo12 = new PdfPCell(new Paragraph(ContractConstants.registration_form_5, content));
            artigo12.setBorderColor(BaseColor.WHITE);
            artigo12.setPaddingLeft(5);
            artigo12.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo12.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo2 = new PdfPCell(new Paragraph(ContractConstants.registration_form_6, normalFontBold));
            artigo2.setBorderColor(BaseColor.WHITE);
            artigo2.setPaddingLeft(5);
            artigo2.setHorizontalAlignment(Element.ALIGN_CENTER);
            artigo2.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo21 = new PdfPCell(new Paragraph(ContractConstants.registration_form_7, content));
            artigo21.setBorderColor(BaseColor.WHITE);
            artigo21.setPaddingLeft(5);
            artigo21.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo21.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo22 = new PdfPCell(new Paragraph(ContractConstants.registration_form_8, content));
            artigo22.setBorderColor(BaseColor.WHITE);
            artigo22.setPaddingLeft(5);
            artigo22.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo22.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo22a = new PdfPCell(new Paragraph(String.format(ContractConstants.registration_form_9, contractInfo.getChannelName()), content));
            artigo22a.setBorderColor(BaseColor.WHITE);
            artigo22a.setPaddingLeft(5);
            artigo22a.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo22a.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo22b = new PdfPCell(new Paragraph(String.format(ContractConstants.registration_form_10, contractInfo.getIsdnWallet()), content));
            artigo22b.setBorderColor(BaseColor.WHITE);
            artigo22b.setPaddingLeft(5);
            artigo22b.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo22b.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo3 = new PdfPCell(new Paragraph(ContractConstants.registration_form_11, normalFontBold));
            artigo3.setBorderColor(BaseColor.WHITE);
            artigo3.setPaddingLeft(5);
            artigo3.setHorizontalAlignment(Element.ALIGN_CENTER);
            artigo3.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo31 = new PdfPCell(new Paragraph(ContractConstants.registration_form_12, content));
            artigo31.setBorderColor(BaseColor.WHITE);
            artigo31.setPaddingLeft(5);
            artigo31.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo31.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo311 = new PdfPCell(new Paragraph(ContractConstants.registration_form_13, content));
            artigo311.setBorderColor(BaseColor.WHITE);
            artigo311.setPaddingLeft(5);
            artigo311.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo311.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo312 = new PdfPCell(new Paragraph(ContractConstants.registration_form_14, content));
            artigo312.setBorderColor(BaseColor.WHITE);
            artigo312.setPaddingLeft(5);
            artigo312.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo312.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo313 = new PdfPCell(new Paragraph(ContractConstants.registration_form_15, content));
            artigo313.setBorderColor(BaseColor.WHITE);
            artigo313.setPaddingLeft(5);
            artigo313.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo313.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo32 = new PdfPCell(new Paragraph(ContractConstants.registration_form_16, content));
            artigo32.setBorderColor(BaseColor.WHITE);
            artigo32.setPaddingLeft(5);
            artigo32.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo32.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo321 = new PdfPCell(new Paragraph(ContractConstants.registration_form_17, content));
            artigo321.setBorderColor(BaseColor.WHITE);
            artigo321.setPaddingLeft(5);
            artigo321.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo321.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo322 = new PdfPCell(new Paragraph(ContractConstants.registration_form_18, content));
            artigo322.setBorderColor(BaseColor.WHITE);
            artigo322.setPaddingLeft(5);
            artigo322.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo322.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo323 = new PdfPCell(new Paragraph(ContractConstants.registration_form_19, content));
            artigo323.setBorderColor(BaseColor.WHITE);
            artigo323.setPaddingLeft(5);
            artigo323.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo323.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo4 = new PdfPCell(new Paragraph(ContractConstants.registration_form_20, normalFontBold));
            artigo4.setBorderColor(BaseColor.WHITE);
            artigo4.setPaddingLeft(5);
            artigo4.setHorizontalAlignment(Element.ALIGN_CENTER);
            artigo4.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo41 = new PdfPCell(new Paragraph(ContractConstants.registration_form_21, content));
            artigo41.setBorderColor(BaseColor.WHITE);
            artigo41.setPaddingLeft(5);
            artigo41.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo41.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo411 = new PdfPCell(new Paragraph(ContractConstants.registration_form_22, content));
            artigo411.setBorderColor(BaseColor.WHITE);
            artigo411.setPaddingLeft(5);
            artigo411.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo411.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo412 = new PdfPCell(new Paragraph(ContractConstants.registration_form_23, content));
            artigo412.setBorderColor(BaseColor.WHITE);
            artigo412.setPaddingLeft(5);
            artigo412.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo412.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo413 = new PdfPCell(new Paragraph(ContractConstants.registration_form_24, content));
            artigo413.setBorderColor(BaseColor.WHITE);
            artigo413.setPaddingLeft(5);
            artigo413.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo413.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo414 = new PdfPCell(new Paragraph(ContractConstants.registration_form_25, content));
            artigo414.setBorderColor(BaseColor.WHITE);
            artigo414.setPaddingLeft(5);
            artigo414.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo414.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo42 = new PdfPCell(new Paragraph(ContractConstants.registration_form_26, content));
            artigo42.setBorderColor(BaseColor.WHITE);
            artigo42.setPaddingLeft(5);
            artigo42.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo42.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo421 = new PdfPCell(new Paragraph(ContractConstants.registration_form_27, content));
            artigo421.setBorderColor(BaseColor.WHITE);
            artigo421.setPaddingLeft(5);
            artigo421.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo421.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo422 = new PdfPCell(new Paragraph(ContractConstants.registration_form_28, content));
            artigo422.setBorderColor(BaseColor.WHITE);
            artigo422.setPaddingLeft(5);
            artigo422.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo422.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo423 = new PdfPCell(new Paragraph(ContractConstants.registration_form_29, content));
            artigo423.setBorderColor(BaseColor.WHITE);
            artigo423.setPaddingLeft(5);
            artigo423.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo423.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo424 = new PdfPCell(new Paragraph(ContractConstants.registration_form_30, content));
            artigo424.setBorderColor(BaseColor.WHITE);
            artigo424.setPaddingLeft(5);
            artigo424.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo424.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo425 = new PdfPCell(new Paragraph(ContractConstants.registration_form_31, content));
            artigo425.setBorderColor(BaseColor.WHITE);
            artigo425.setPaddingLeft(5);
            artigo425.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo425.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo5 = new PdfPCell(new Paragraph(ContractConstants.registration_form_32, normalFontBold));
            artigo5.setBorderColor(BaseColor.WHITE);
            artigo5.setPaddingLeft(5);
            artigo5.setHorizontalAlignment(Element.ALIGN_CENTER);
            artigo5.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo50 = new PdfPCell(new Paragraph(ContractConstants.registration_form_33, content));
            artigo50.setBorderColor(BaseColor.WHITE);
            artigo50.setPaddingLeft(5);
            artigo50.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo50.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo51 = new PdfPCell(new Paragraph(ContractConstants.registration_form_34, content));
            artigo51.setBorderColor(BaseColor.WHITE);
            artigo51.setPaddingLeft(5);
            artigo51.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo51.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo52 = new PdfPCell(new Paragraph(ContractConstants.registration_form_35, content));
            artigo52.setBorderColor(BaseColor.WHITE);
            artigo52.setPaddingLeft(5);
            artigo52.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo52.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo53 = new PdfPCell(new Paragraph(ContractConstants.registration_form_36, content));
            artigo53.setBorderColor(BaseColor.WHITE);
            artigo53.setPaddingLeft(5);
            artigo53.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo53.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo6 = new PdfPCell(new Paragraph(ContractConstants.registration_form_37, normalFontBold));
            artigo6.setBorderColor(BaseColor.WHITE);
            artigo6.setPaddingLeft(5);
            artigo6.setHorizontalAlignment(Element.ALIGN_CENTER);
            artigo6.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo61 = new PdfPCell(new Paragraph(ContractConstants.registration_form_38, content));
            artigo61.setBorderColor(BaseColor.WHITE);
            artigo61.setPaddingLeft(5);
            artigo61.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo61.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo62 = new PdfPCell(new Paragraph(ContractConstants.registration_form_39, content));
            artigo62.setBorderColor(BaseColor.WHITE);
            artigo62.setPaddingLeft(5);
            artigo62.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo62.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell artigo63 = new PdfPCell(new Paragraph(ContractConstants.registration_form_40, content));
            artigo63.setBorderColor(BaseColor.WHITE);
            artigo63.setPaddingLeft(5);
            artigo63.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            artigo63.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell signA = new PdfPCell(new Paragraph(ContractConstants.registration_form_41, normalFontBold));
            signA.setBorderColor(BaseColor.WHITE);
            signA.setPaddingLeft(10);
            signA.setHorizontalAlignment(Element.ALIGN_CENTER);
            signA.setVerticalAlignment(Element.ALIGN_CENTER);
            signA.setPaddingBottom(5);

            PdfPCell signB = new PdfPCell(new Paragraph(ContractConstants.registration_form_42, normalFontBold));
            signB.setBorderColor(BaseColor.WHITE);
            signB.setPaddingLeft(10);
            signB.setPaddingBottom(5);
            signB.setHorizontalAlignment(Element.ALIGN_CENTER);
            signB.setVerticalAlignment(Element.ALIGN_CENTER);

//            Image imageA = Image.getInstance(getBytes("D:\\Restart\\ThreadContainer\\etc\\imgSignPartA.png"));
            Image imageA = Image.getInstance(getBytes(basePath + "/etc/imgSignPartA.png"));
            imageA.scaleAbsolute(60, 40);
            PdfPCell imageSignA = new PdfPCell(imageA);
            imageSignA.setBorderColor(BaseColor.WHITE);
            imageSignA.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageSignA.setVerticalAlignment(Element.ALIGN_MIDDLE);

            Image imageB = Image.getInstance(getBytes(pathSignB));
            imageB.scaleAbsolute(120, 90);
            PdfPCell imageSignB = new PdfPCell(imageB);
            imageSignB.setBorderColor(BaseColor.WHITE);
            imageSignB.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageSignB.setVerticalAlignment(Element.ALIGN_MIDDLE);

            Image imageFrontBI = Image.getInstance(getBytes(pathFrontBI));
            imageFrontBI.scaleAbsolute(120, 90);
            PdfPCell cellFrontBI = new PdfPCell(imageFrontBI);
            cellFrontBI.setBorderColor(BaseColor.WHITE);
            cellFrontBI.setPaddingBottom(15f);
            cellFrontBI.setHorizontalAlignment(Element.ALIGN_CENTER);
            cellFrontBI.setVerticalAlignment(Element.ALIGN_MIDDLE);

            Image imageBackBI = Image.getInstance(getBytes(pathBackBI));
            imageBackBI.scaleAbsolute(120, 90);
            PdfPCell cellBackBI = new PdfPCell(imageBackBI);
            cellBackBI.setBorderColor(BaseColor.WHITE);
            cellBackBI.setHorizontalAlignment(Element.ALIGN_CENTER);
            cellBackBI.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPTable table0 = new PdfPTable(1); // 3 columns.
            table0.addCell(title);

            PdfPTable table1 = new PdfPTable(1); // 3 columns.
            table1.addCell(contractNo);

            PdfPTable table2 = new PdfPTable(1); // 3 columns.
            table2.addCell(dateTime);

            float[] baseColumn = new float[]{0.2f, 0.2f};

            PdfPTable table6 = new PdfPTable(2); // 3 columns.
            table6.setSpacingBefore(15f); // Space before table
            table6.setSpacingAfter(15f);
            table6.setWidths(baseColumn);

            table6.addCell(tableA);
            table6.addCell(tableAContent);

            table6.addCell(tableARepresentante);
            table6.addCell(tableARepresentanteContent);

            table6.addCell(tableACargo);
            table6.addCell(tableACargoContent);

            table6.addCell(tableAEndereco);
            table6.addCell(tableAEnderecoContent);

            table6.addCell(tableANuit);
            table6.addCell(tableANuitContent);

            PdfPTable table7 = new PdfPTable(2); // 3 columns.
            table7.setSpacingAfter(15f);
            table7.setWidths(baseColumn);

            table7.addCell(tableB);
            table7.addCell(tableBContent);
            table7.addCell(tableBRepresentante);
            table7.addCell(tableBRepresentanteContent);

            table7.addCell(tableBBINumber);
            table7.addCell(tableBBINumberContent);

            table7.addCell(tableBValidoAte);
            table7.addCell(tableBValidoAteContent);

            table7.addCell(tableBEndereco);
            table7.addCell(tableBEnderecoContent);

            table7.addCell(tableBPhone);
            table7.addCell(tableBPhoneContent);

            table7.addCell(tableBPhoneEmola);
            table7.addCell(tableBPhoneEmolaContent);

            PdfPTable table8 = new PdfPTable(1); // 3 columns.
            table8.addCell(contentAfterTableB);

            PdfPTable table9 = new PdfPTable(1); // 3 columns.
            table9.addCell(artigo1);
            table9.addCell(artigo11);
            table9.addCell(artigo12);
            table9.addCell(artigo2);
            table9.addCell(artigo21);
            table9.addCell(artigo22);
            table9.addCell(artigo22a);
            table9.addCell(artigo22b);
            table9.addCell(artigo3);
            table9.addCell(artigo31);
            table9.addCell(artigo311);
            table9.addCell(artigo312);
            table9.addCell(artigo313);
            table9.addCell(artigo32);
            table9.addCell(artigo321);
            table9.addCell(artigo322);
            table9.addCell(artigo323);
            table9.addCell(artigo4);
            table9.addCell(artigo41);
            table9.addCell(artigo411);
            table9.addCell(artigo412);
            table9.addCell(artigo413);
            table9.addCell(artigo414);
            table9.addCell(artigo42);
            table9.addCell(artigo421);
            table9.addCell(artigo422);
            table9.addCell(artigo423);
            table9.addCell(artigo424);
            table9.addCell(artigo425);
            table9.addCell(artigo5);
            table9.addCell(artigo50);
            table9.addCell(artigo51);
            table9.addCell(artigo52);
            table9.addCell(artigo53);
            table9.addCell(artigo6);
            table9.addCell(artigo61);
            table9.addCell(artigo62);
            table9.addCell(artigo63);

            PdfPTable table10 = new PdfPTable(2); // 3 columns.
            table10.setSpacingBefore(15f); // Space before table
            table10.setSpacingAfter(15f);
            table10.addCell(signA);
            table10.addCell(signB);
            table10.addCell(imageSignA);
            table10.addCell(imageSignB);

            PdfPTable table11 = new PdfPTable(1); // 3 columns.
            table11.addCell(cellFrontBI);
            table11.setSpacingBefore(15.0f);
            table11.addCell(cellBackBI);

            document.add(table0);
            document.add(table1);
            document.add(table2);
            document.add(table6);
            document.add(table7);
            document.add(table8);
            document.add(table9);
            document.add(table10);
            document.newPage();
            document.add(table11);

            document.close();
            output.close();
            FileInputStream fis = new FileInputStream(myFile);
            ftpClient.storeFile(myFile.getName(), fis);
            fis.close();
            myFile.delete();
//            pdfWriter.close();

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (DocumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
        }
    }

    public void createEMolaContract(RequestChannel contractInfo, String sysdate, String imgSignClient, FTPClient ftpClient, String basePath) throws IOException {

        Font header = FontFactory.getFont(FontFactory.TIMES, 14, Font.BOLD,
                new CMYKColor(0, 0, 0, 255));
        Font eMolaFontBold = FontFactory.getFont(FontFactory.TIMES, 10, Font.BOLD,
                new CMYKColor(0, 0, 0, 255));

        Font eMolaFontNormal = FontFactory.getFont(FontFactory.TIMES, 8, Font.NORMAL,
                new CMYKColor(0, 0, 0, 255));
        Font content = FontFactory.getFont(FontFactory.TIMES, 8, Font.NORMAL, BaseColor.BLACK);
        File myFile = new File(path);
        FileOutputStream output = null;
        try {
            output = new FileOutputStream(myFile);
            // Step 2
            PdfWriter pdfWriter = PdfWriter.getInstance(document, output);
            // Step 3
            document.open();
            // Step 4 Add content
            Image imageLogo = Image.getInstance(getBytes(basePath + "/etc/logo_emola.png"));
            imageLogo.scaleAbsolute(120, 40);
            PdfPCell imageEmolaLogo = new PdfPCell(imageLogo);
            imageEmolaLogo.setPaddingTop(15f);
            imageEmolaLogo.setBorderColor(BaseColor.WHITE);
            imageEmolaLogo.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageEmolaLogo.setVerticalAlignment(Element.ALIGN_MIDDLE);


            /* Hien thi tieu de */
            PdfPCell title = new PdfPCell(new Paragraph(ContractConstants.emola_form_1, header));
            title.setBorderColor(BaseColor.WHITE);
            title.setPaddingLeft(5);
            title.setPaddingBottom(15f);
            title.setHorizontalAlignment(Element.ALIGN_CENTER);
            title.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell dateTime = new PdfPCell(new Paragraph(String.format(ContractConstants.registration_date_time, sysdate.substring(6, 8), sysdate.substring(4, 6), sysdate.substring(0, 4)), header));
            dateTime.setBorderColor(BaseColor.WHITE);
            dateTime.setPaddingBottom(15f);
            dateTime.setHorizontalAlignment(Element.ALIGN_CENTER);
            dateTime.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell titleTable1 = new PdfPCell(new Paragraph(ContractConstants.emola_form_2, content));
            titleTable1.setBorderColor(BaseColor.BLACK);
            titleTable1.setHorizontalAlignment(Element.ALIGN_CENTER);
            titleTable1.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1.setPaddingBottom(5);

            PdfPCell nameMaster = new PdfPCell(new Paragraph(ContractConstants.emola_form_3, content));
            nameMaster.setBorderColor(BaseColor.BLACK);
            nameMaster.setPaddingLeft(10);
            nameMaster.setPaddingBottom(5);
            nameMaster.setHorizontalAlignment(Element.ALIGN_LEFT);
            nameMaster.setVerticalAlignment(Element.ALIGN_CENTER);

            PdfPCell nameMasterContent = new PdfPCell(new Paragraph(ContractConstants.registration_movitel_sa, content));
            nameMasterContent.setBorderColor(BaseColor.BLACK);
            nameMasterContent.setPaddingLeft(10);
            nameMasterContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            nameMasterContent.setVerticalAlignment(Element.ALIGN_CENTER);
            nameMasterContent.setPaddingBottom(5);

            PdfPCell nameMasterAgent = new PdfPCell(new Paragraph(ContractConstants.emola_form_4, content));
            nameMasterAgent.setBorderColor(BaseColor.BLACK);
            nameMasterAgent.setPaddingLeft(10);
            nameMasterAgent.setPaddingBottom(5);
            nameMasterAgent.setHorizontalAlignment(Element.ALIGN_LEFT);
            nameMasterAgent.setVerticalAlignment(Element.ALIGN_CENTER);

            PdfPCell nameMasterAgentContent = new PdfPCell(new Paragraph(ContractConstants.registration_sr_nguyen_dat, content));
            nameMasterAgentContent.setBorderColor(BaseColor.BLACK);
            nameMasterAgentContent.setPaddingLeft(10);
            nameMasterAgentContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            nameMasterAgentContent.setVerticalAlignment(Element.ALIGN_CENTER);
            nameMasterAgentContent.setPaddingBottom(5);

            PdfPCell tableAEndereco = new PdfPCell(new Paragraph(ContractConstants.registration_address, content));
            tableAEndereco.setBorderColor(BaseColor.BLACK);
            tableAEndereco.setPaddingLeft(10);
            tableAEndereco.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableAEndereco.setVerticalAlignment(Element.ALIGN_CENTER);
            tableAEndereco.setPaddingBottom(5);

            PdfPCell tableAEnderecoContent = new PdfPCell(new Paragraph(ContractConstants.registration_address_part_a, content));
            tableAEnderecoContent.setBorderColor(BaseColor.BLACK);
            tableAEnderecoContent.setPaddingLeft(10);
            tableAEnderecoContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableAEnderecoContent.setVerticalAlignment(Element.ALIGN_CENTER);
            tableAEnderecoContent.setPaddingBottom(5);

            PdfPCell contact = new PdfPCell(new Paragraph(ContractConstants.emola_form_5, content));
            contact.setBorderColor(BaseColor.BLACK);
            contact.setPaddingLeft(10);
            contact.setHorizontalAlignment(Element.ALIGN_LEFT);
            contact.setVerticalAlignment(Element.ALIGN_CENTER);
            contact.setPaddingBottom(5);

            PdfPCell contactContent = new PdfPCell(new Paragraph("+258.879778899", content));
            contactContent.setBorderColor(BaseColor.BLACK);
            contactContent.setPaddingLeft(10);
            contactContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            contactContent.setVerticalAlignment(Element.ALIGN_CENTER);
            contactContent.setPaddingBottom(5);
//
            PdfPCell email = new PdfPCell(new Paragraph("E-mail", content));
            email.setBorderColor(BaseColor.BLACK);
            email.setPaddingLeft(10);
            email.setHorizontalAlignment(Element.ALIGN_LEFT);
            email.setVerticalAlignment(Element.ALIGN_CENTER);
            email.setPaddingBottom(5);

            PdfPCell emailContent = new PdfPCell(new Paragraph("emola.cc@movitel.co.mz", content));
            emailContent.setBorderColor(BaseColor.BLACK);
            emailContent.setPaddingLeft(10);
            emailContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            emailContent.setVerticalAlignment(Element.ALIGN_CENTER);
            emailContent.setPaddingBottom(5);

            PdfPCell titleTable4 = new PdfPCell(new Paragraph(ContractConstants.emola_form_6, content));
            titleTable4.setBorderColor(BaseColor.BLACK);
            titleTable4.setHorizontalAlignment(Element.ALIGN_CENTER);
            titleTable4.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable4.setPaddingBottom(5);

            PdfPCell table4ShopName = new PdfPCell(new Paragraph(ContractConstants.emola_form_7, content));
            table4ShopName.setBorderColor(BaseColor.BLACK);
            table4ShopName.setPaddingLeft(10);
            table4ShopName.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4ShopName.setVerticalAlignment(Element.ALIGN_CENTER);
            table4ShopName.setPaddingBottom(5);

            PdfPCell table4ShopNameContent = new PdfPCell(new Paragraph(contractInfo.getChannelName(), content));
            table4ShopNameContent.setBorderColor(BaseColor.BLACK);
            table4ShopNameContent.setPaddingLeft(10);
            table4ShopNameContent.setPaddingBottom(5);
            table4ShopNameContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4ShopNameContent.setVerticalAlignment(Element.ALIGN_CENTER);
//
            PdfPCell table4BusinessType = new PdfPCell(new Paragraph(ContractConstants.emola_form_8, content));
            table4BusinessType.setBorderColor(BaseColor.BLACK);
            table4BusinessType.setPaddingLeft(10);
            table4BusinessType.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4BusinessType.setVerticalAlignment(Element.ALIGN_CENTER);
            table4BusinessType.setPaddingBottom(5);

            PdfPCell table4BusinessTypeContent = new PdfPCell(new Paragraph("", content));
            table4BusinessTypeContent.setBorderColor(BaseColor.BLACK);
            table4BusinessTypeContent.setPaddingLeft(10);
            table4BusinessTypeContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4BusinessTypeContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4BusinessTypeContent.setPaddingBottom(5);
//
            PdfPCell table4Endereco = new PdfPCell(new Paragraph(ContractConstants.registration_address, content));
            table4Endereco.setBorderColor(BaseColor.BLACK);
            table4Endereco.setPaddingLeft(10);
            table4Endereco.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4Endereco.setVerticalAlignment(Element.ALIGN_CENTER);
            table4Endereco.setPaddingBottom(5);

            PdfPCell table4EnderecoContent = new PdfPCell(new Paragraph(contractInfo.getAddress(), content));
            table4EnderecoContent.setBorderColor(BaseColor.BLACK);
            table4EnderecoContent.setPaddingLeft(10);
            table4EnderecoContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4EnderecoContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4EnderecoContent.setPaddingBottom(5);

            PdfPCell table4Province = new PdfPCell(new Paragraph(ContractConstants.emola_form_9, content));
            table4Province.setBorderColor(BaseColor.BLACK);
            table4Province.setPaddingLeft(10);
            table4Province.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4Province.setVerticalAlignment(Element.ALIGN_CENTER);
            table4Province.setPaddingBottom(5);

            PdfPCell table4ProvinceContent = new PdfPCell(new Paragraph(contractInfo.getAddress(), content));
            table4ProvinceContent.setBorderColor(BaseColor.BLACK);
            table4ProvinceContent.setPaddingLeft(10);
            table4ProvinceContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4ProvinceContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4ProvinceContent.setPaddingBottom(5);

            PdfPCell table4ContactPerson = new PdfPCell(new Paragraph(ContractConstants.emola_form_10, content));
            table4ContactPerson.setBorderColor(BaseColor.BLACK);
            table4ContactPerson.setPaddingLeft(10);
            table4ContactPerson.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4ContactPerson.setVerticalAlignment(Element.ALIGN_CENTER);
            table4ContactPerson.setPaddingBottom(5);

            PdfPCell table4ContactPersonContent = new PdfPCell(new Paragraph(contractInfo.getChannelName(), content));
            table4ContactPersonContent.setBorderColor(BaseColor.BLACK);
            table4ContactPersonContent.setPaddingLeft(10);
            table4ContactPersonContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4ContactPersonContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4ContactPersonContent.setPaddingBottom(5);

            PdfPCell table4DOB = new PdfPCell(new Paragraph(ContractConstants.emola_form_11, content));
            table4DOB.setBorderColor(BaseColor.BLACK);
            table4DOB.setPaddingLeft(10);
            table4DOB.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4DOB.setVerticalAlignment(Element.ALIGN_CENTER);
            table4DOB.setPaddingBottom(5);

            PdfPCell table4DOBContent = new PdfPCell(new Paragraph("", content));
            table4DOBContent.setBorderColor(BaseColor.BLACK);
            table4DOBContent.setPaddingLeft(10);
            table4DOBContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4DOBContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4DOBContent.setPaddingBottom(5);

            PdfPCell table4PhoneEmola = new PdfPCell(new Paragraph(ContractConstants.emola_form_12, content));
            table4PhoneEmola.setBorderColor(BaseColor.BLACK);
            table4PhoneEmola.setPaddingLeft(10);
            table4PhoneEmola.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4PhoneEmola.setVerticalAlignment(Element.ALIGN_CENTER);
            table4PhoneEmola.setPaddingBottom(5);

            PdfPCell table4PhoneEmolaContent = new PdfPCell(new Paragraph(contractInfo.getIsdnWallet(), content));
            table4PhoneEmolaContent.setBorderColor(BaseColor.BLACK);
            table4PhoneEmolaContent.setPaddingLeft(10);
            table4PhoneEmolaContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4PhoneEmolaContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4PhoneEmolaContent.setPaddingBottom(5);

            PdfPCell table4Email = new PdfPCell(new Paragraph("E-mail", content));
            table4Email.setBorderColor(BaseColor.BLACK);
            table4Email.setPaddingLeft(10);
            table4Email.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4Email.setVerticalAlignment(Element.ALIGN_CENTER);
            table4Email.setPaddingBottom(5);

            PdfPCell table4EmailContent = new PdfPCell(new Paragraph("", content));
            table4EmailContent.setBorderColor(BaseColor.BLACK);
            table4EmailContent.setPaddingLeft(10);
            table4EmailContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4EmailContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4EmailContent.setPaddingBottom(5);

            PdfPCell table4Nuit = new PdfPCell(new Paragraph(ContractConstants.registration_nuit, content));
            table4Nuit.setBorderColor(BaseColor.BLACK);
            table4Nuit.setPaddingLeft(10);
            table4Nuit.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4Nuit.setVerticalAlignment(Element.ALIGN_CENTER);
            table4Nuit.setPaddingBottom(5);

            PdfPCell table4NuitContent = new PdfPCell(new Paragraph(contractInfo.getNuitNumber(), content));
            table4NuitContent.setBorderColor(BaseColor.BLACK);
            table4NuitContent.setPaddingLeft(10);
            table4NuitContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table4NuitContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table4NuitContent.setPaddingBottom(5);

            PdfPCell titleTable5 = new PdfPCell(new Paragraph(ContractConstants.emola_form_13, content));
            titleTable5.setBorderColor(BaseColor.BLACK);
            titleTable5.setHorizontalAlignment(Element.ALIGN_CENTER);
            titleTable5.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable5.setPaddingBottom(5);

            PdfPCell table6NameEmola = new PdfPCell(new Paragraph(ContractConstants.emola_form_14, content));
            table6NameEmola.setBorderColor(BaseColor.BLACK);
            table6NameEmola.setPaddingLeft(10);
            table6NameEmola.setHorizontalAlignment(Element.ALIGN_LEFT);
            table6NameEmola.setVerticalAlignment(Element.ALIGN_CENTER);
            table6NameEmola.setPaddingBottom(5);

            PdfPCell table6NameEmolaContent = new PdfPCell(new Paragraph("Pham Huy Cuong (Director E-MOLA)", content));
            table6NameEmolaContent.setBorderColor(BaseColor.BLACK);
            table6NameEmolaContent.setPaddingLeft(10);
            table6NameEmolaContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table6NameEmolaContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table6NameEmolaContent.setPaddingBottom(5);
//
//
            PdfPCell table6Signature = new PdfPCell(new Paragraph(ContractConstants.emola_form_15, content));
            table6Signature.setBorderColor(BaseColor.BLACK);
            table6Signature.setPaddingLeft(10);
            table6Signature.setHorizontalAlignment(Element.ALIGN_LEFT);
            table6Signature.setVerticalAlignment(Element.ALIGN_MIDDLE);
            table6Signature.setPaddingBottom(5);
//
//            Image imageDirectorEmola = Image.getInstance(getBytes("D:\\Restart\\ThreadContainer\\etc\\imgDirectorEmolaSign.jpeg"));
            Image imageDirectorEmola = Image.getInstance(getBytes(basePath + "/etc/imgDirectorEmolaSign.jpeg"));
            imageDirectorEmola.scaleAbsolute(60, 40);
            PdfPCell imageSignEmola = new PdfPCell(imageDirectorEmola);
            imageSignEmola.setPaddingBottom(10f);
            imageSignEmola.setPaddingTop(10f);
            imageSignEmola.setBorderColor(BaseColor.BLACK);
            imageSignEmola.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageSignEmola.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell titleTable7 = new PdfPCell(new Paragraph(ContractConstants.emola_form_16, content));
            titleTable7.setBorderColor(BaseColor.BLACK);
            titleTable7.setHorizontalAlignment(Element.ALIGN_CENTER);
            titleTable7.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable7.setPaddingBottom(5);

            PdfPCell table8NameEmola = new PdfPCell(new Paragraph(ContractConstants.emola_form_14, content));
            table8NameEmola.setBorderColor(BaseColor.BLACK);
            table8NameEmola.setPaddingLeft(10);
            table8NameEmola.setHorizontalAlignment(Element.ALIGN_LEFT);
            table8NameEmola.setVerticalAlignment(Element.ALIGN_CENTER);
            table8NameEmola.setPaddingBottom(5);

            PdfPCell table8NameEmolaContent = new PdfPCell(new Paragraph("Sr Nguyen Dat", content));
            table8NameEmolaContent.setBorderColor(BaseColor.BLACK);
            table8NameEmolaContent.setPaddingLeft(10);
            table8NameEmolaContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table8NameEmolaContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table8NameEmolaContent.setPaddingBottom(5);
//
//
            PdfPCell table8Signature = new PdfPCell(new Paragraph(ContractConstants.emola_form_15, content));
            table8Signature.setBorderColor(BaseColor.BLACK);
            table8Signature.setPaddingLeft(10);
            table8Signature.setHorizontalAlignment(Element.ALIGN_LEFT);
            table8Signature.setVerticalAlignment(Element.ALIGN_MIDDLE);
            table8Signature.setPaddingBottom(5);
//
//            Image imageSignMasterAgent = Image.getInstance(getBytes("D:\\Restart\\ThreadContainer\\etc\\imgSignPartA.png"));
            Image imageSignMasterAgent = Image.getInstance(getBytes(basePath + "/etc/imgSignPartA.png"));
            imageSignMasterAgent.scaleAbsolute(60, 40);
            PdfPCell imageTable8SignEmola = new PdfPCell(imageSignMasterAgent);
            imageTable8SignEmola.setPaddingBottom(10f);
            imageTable8SignEmola.setPaddingTop(10f);
            imageTable8SignEmola.setBorderColor(BaseColor.BLACK);
            imageTable8SignEmola.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageTable8SignEmola.setVerticalAlignment(Element.ALIGN_MIDDLE);


            PdfPCell titleTable9 = new PdfPCell(new Paragraph(ContractConstants.emola_form_17, content));
            titleTable9.setBorderColor(BaseColor.BLACK);
            titleTable9.setHorizontalAlignment(Element.ALIGN_CENTER);
            titleTable9.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable9.setPaddingBottom(5);

            PdfPCell table10NameEmola = new PdfPCell(new Paragraph(ContractConstants.emola_form_14, content));
            table10NameEmola.setBorderColor(BaseColor.BLACK);
            table10NameEmola.setPaddingLeft(10);
            table10NameEmola.setHorizontalAlignment(Element.ALIGN_LEFT);
            table10NameEmola.setVerticalAlignment(Element.ALIGN_CENTER);
            table10NameEmola.setPaddingBottom(5);

            PdfPCell table10NameEmolaContent = new PdfPCell(new Paragraph(contractInfo.getChannelName(), content));
            table10NameEmolaContent.setBorderColor(BaseColor.BLACK);
            table10NameEmolaContent.setPaddingLeft(10);
            table10NameEmolaContent.setHorizontalAlignment(Element.ALIGN_LEFT);
            table10NameEmolaContent.setVerticalAlignment(Element.ALIGN_CENTER);
            table10NameEmolaContent.setPaddingBottom(5);
//
//
            PdfPCell table10Signature = new PdfPCell(new Paragraph(ContractConstants.emola_form_15, content));
            table10Signature.setBorderColor(BaseColor.BLACK);
            table10Signature.setPaddingLeft(10);
            table10Signature.setHorizontalAlignment(Element.ALIGN_LEFT);
            table10Signature.setVerticalAlignment(Element.ALIGN_MIDDLE);
            table10Signature.setPaddingBottom(5);
//
            Image imageSignAgentEmola = Image.getInstance(getBytes(imgSignClient));
            imageSignAgentEmola.scaleAbsolute(60, 40);
            PdfPCell imageTable10SignEmola = new PdfPCell(imageSignAgentEmola);
            imageTable10SignEmola.setPaddingBottom(10f);
            imageTable10SignEmola.setPaddingTop(10f);
            imageTable10SignEmola.setBorderColor(BaseColor.BLACK);
            imageTable10SignEmola.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageTable10SignEmola.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell titleTable11 = new PdfPCell(new Paragraph(ContractConstants.emola_form_18, eMolaFontBold));
            titleTable11.setBorderColor(BaseColor.WHITE);
            titleTable11.setHorizontalAlignment(Element.ALIGN_CENTER);
            titleTable11.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable11.setPaddingBottom(5);

            PdfPCell titleTable111 = new PdfPCell(new Paragraph(ContractConstants.emola_form_19, eMolaFontNormal));
            titleTable111.setBorderColor(BaseColor.WHITE);
            titleTable111.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable111.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable111.setPaddingBottom(5);

            PdfPCell titleTable112 = new PdfPCell(new Paragraph(ContractConstants.emola_form_20, eMolaFontNormal));
            titleTable112.setBorderColor(BaseColor.WHITE);
            titleTable112.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable112.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable112.setPaddingBottom(5);

            PdfPCell titleTable113 = new PdfPCell(new Paragraph(ContractConstants.emola_form_21, eMolaFontNormal));
            titleTable113.setBorderColor(BaseColor.WHITE);
            titleTable113.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable113.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable113.setPaddingBottom(5);

            PdfPCell titleTable114 = new PdfPCell(new Paragraph(ContractConstants.emola_form_22, eMolaFontNormal));
            titleTable114.setBorderColor(BaseColor.WHITE);
            titleTable114.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable114.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable114.setPaddingBottom(5);

            PdfPCell titleTable115 = new PdfPCell(new Paragraph(ContractConstants.emola_form_23, eMolaFontNormal));
            titleTable115.setBorderColor(BaseColor.WHITE);
            titleTable115.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable115.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable115.setPaddingBottom(5);

            PdfPCell titleTable116 = new PdfPCell(new Paragraph(ContractConstants.emola_form_24, eMolaFontNormal));
            titleTable116.setBorderColor(BaseColor.WHITE);
            titleTable116.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable116.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable116.setPaddingBottom(5);

            PdfPCell titleTable117 = new PdfPCell(new Paragraph(ContractConstants.emola_form_25, eMolaFontNormal));
            titleTable117.setBorderColor(BaseColor.WHITE);
            titleTable117.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable117.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable117.setPaddingBottom(5);

            PdfPCell titleTable118 = new PdfPCell(new Paragraph(ContractConstants.emola_form_26, eMolaFontNormal));
            titleTable118.setBorderColor(BaseColor.WHITE);
            titleTable118.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable118.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable118.setPaddingBottom(5);

            PdfPCell titleTable119 = new PdfPCell(new Paragraph(ContractConstants.emola_form_27, eMolaFontNormal));
            titleTable119.setBorderColor(BaseColor.WHITE);
            titleTable119.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable119.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable119.setPaddingBottom(5);

            PdfPCell titleTable1110 = new PdfPCell(new Paragraph(ContractConstants.emola_form_28, eMolaFontNormal));
            titleTable1110.setBorderColor(BaseColor.WHITE);
            titleTable1110.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1110.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1110.setPaddingBottom(5);

            PdfPCell titleTable1111 = new PdfPCell(new Paragraph(ContractConstants.emola_form_29, eMolaFontNormal));
            titleTable1111.setBorderColor(BaseColor.WHITE);
            titleTable1111.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1111.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1111.setPaddingBottom(5);

            PdfPCell titleTable1112 = new PdfPCell(new Paragraph(ContractConstants.emola_form_30, eMolaFontNormal));
            titleTable1112.setBorderColor(BaseColor.WHITE);
            titleTable1112.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1112.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1112.setPaddingBottom(5);

            PdfPCell titleTable1113 = new PdfPCell(new Paragraph(ContractConstants.emola_form_31, eMolaFontNormal));
            titleTable1113.setBorderColor(BaseColor.WHITE);
            titleTable1113.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1113.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1113.setPaddingBottom(5);

            PdfPCell titleTable1114 = new PdfPCell(new Paragraph(ContractConstants.emola_form_32, eMolaFontNormal));
            titleTable1114.setBorderColor(BaseColor.WHITE);
            titleTable1114.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1114.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1114.setPaddingBottom(5);

            PdfPCell titleTable1115 = new PdfPCell(new Paragraph(ContractConstants.emola_form_33, eMolaFontNormal));
            titleTable1115.setBorderColor(BaseColor.WHITE);
            titleTable1115.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1115.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1115.setPaddingBottom(5);

            PdfPCell titleTable1116 = new PdfPCell(new Paragraph(ContractConstants.emola_form_34, eMolaFontNormal));
            titleTable1116.setBorderColor(BaseColor.WHITE);
            titleTable1116.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1116.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1116.setPaddingBottom(5);

            PdfPCell titleTable1117 = new PdfPCell(new Paragraph(ContractConstants.emola_form_35, eMolaFontNormal));
            titleTable1117.setBorderColor(BaseColor.WHITE);
            titleTable1117.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1117.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1117.setPaddingBottom(5);

            PdfPCell titleTable1118 = new PdfPCell(new Paragraph(ContractConstants.emola_form_36, eMolaFontNormal));
            titleTable1118.setBorderColor(BaseColor.WHITE);
            titleTable1118.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1118.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1118.setPaddingBottom(5);

            PdfPCell titleTable1119 = new PdfPCell(new Paragraph(ContractConstants.emola_form_37, eMolaFontNormal));
            titleTable1119.setBorderColor(BaseColor.WHITE);
            titleTable1119.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1119.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1119.setPaddingBottom(5);

            PdfPCell titleTable1120 = new PdfPCell(new Paragraph(ContractConstants.emola_form_38, eMolaFontNormal));
            titleTable1120.setBorderColor(BaseColor.WHITE);
            titleTable1120.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1120.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1120.setPaddingBottom(5);

            PdfPCell titleTable1121 = new PdfPCell(new Paragraph(ContractConstants.emola_form_39, eMolaFontNormal));
            titleTable1121.setBorderColor(BaseColor.WHITE);
            titleTable1121.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1121.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1121.setPaddingBottom(5);

            PdfPCell titleTable1122 = new PdfPCell(new Paragraph(ContractConstants.emola_form_40, eMolaFontNormal));
            titleTable1122.setBorderColor(BaseColor.WHITE);
            titleTable1122.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1122.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1122.setPaddingBottom(5);

            PdfPCell titleTable1123 = new PdfPCell(new Paragraph(ContractConstants.emola_form_41, eMolaFontNormal));
            titleTable1123.setBorderColor(BaseColor.WHITE);
            titleTable1123.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1123.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1123.setPaddingBottom(5);

            PdfPCell titleTable1124 = new PdfPCell(new Paragraph(ContractConstants.emola_form_42, eMolaFontNormal));
            titleTable1124.setBorderColor(BaseColor.WHITE);
            titleTable1124.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1124.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1124.setPaddingBottom(5);

            PdfPCell titleTable1125 = new PdfPCell(new Paragraph(ContractConstants.emola_form_43, eMolaFontNormal));
            titleTable1125.setBorderColor(BaseColor.WHITE);
            titleTable1125.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1125.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1125.setPaddingBottom(5);

            PdfPCell titleTable1126 = new PdfPCell(new Paragraph(ContractConstants.emola_form_44, eMolaFontNormal));
            titleTable1126.setBorderColor(BaseColor.WHITE);
            titleTable1126.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1126.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1126.setPaddingBottom(5);

            PdfPCell titleTable1127 = new PdfPCell(new Paragraph(ContractConstants.emola_form_45, eMolaFontNormal));
            titleTable1127.setBorderColor(BaseColor.WHITE);
            titleTable1127.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1127.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1127.setPaddingBottom(5);

            PdfPCell titleTable1128 = new PdfPCell(new Paragraph(ContractConstants.emola_form_46, eMolaFontNormal));
            titleTable1128.setBorderColor(BaseColor.WHITE);
            titleTable1128.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1128.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1128.setPaddingBottom(5);

            PdfPCell titleTable1129 = new PdfPCell(new Paragraph(ContractConstants.emola_form_47, eMolaFontNormal));
            titleTable1129.setBorderColor(BaseColor.WHITE);
            titleTable1129.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1129.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1129.setPaddingBottom(5);

            PdfPCell titleTable1130 = new PdfPCell(new Paragraph(ContractConstants.emola_form_48, eMolaFontNormal));
            titleTable1130.setBorderColor(BaseColor.WHITE);
            titleTable1130.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1130.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1130.setPaddingBottom(5);

            PdfPCell titleTable1131 = new PdfPCell(new Paragraph(ContractConstants.emola_form_49, eMolaFontNormal));
            titleTable1131.setBorderColor(BaseColor.WHITE);
            titleTable1131.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1131.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1131.setPaddingBottom(5);

            PdfPCell titleTable1132 = new PdfPCell(new Paragraph(ContractConstants.emola_form_50, eMolaFontNormal));
            titleTable1132.setBorderColor(BaseColor.WHITE);
            titleTable1132.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1132.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1132.setPaddingBottom(5);

            PdfPCell titleTable1133 = new PdfPCell(new Paragraph(ContractConstants.emola_form_51, eMolaFontNormal));
            titleTable1133.setBorderColor(BaseColor.WHITE);
            titleTable1133.setHorizontalAlignment(Element.ALIGN_JUSTIFIED);
            titleTable1133.setVerticalAlignment(Element.ALIGN_CENTER);
            titleTable1133.setPaddingBottom(5);

            PdfPTable tableLogo = new PdfPTable(1); // 3 columns.
            tableLogo.addCell(imageEmolaLogo);

            PdfPTable table0 = new PdfPTable(1); // 3 columns.
            table0.addCell(title);
            table0.addCell(dateTime);
            float[] baseColumn = new float[]{0.2f, 0.2f};


            PdfPTable table1 = new PdfPTable(1); // 3 columns.
            table1.addCell(titleTable1);


            PdfPTable table2 = new PdfPTable(2); // 3 columns.
            table2.setWidths(baseColumn);
            table2.addCell(nameMaster);
            table2.addCell(nameMasterContent);
            table2.addCell(nameMasterAgent);
            table2.addCell(nameMasterAgentContent);
            table2.addCell(tableAEndereco);
            table2.addCell(tableAEnderecoContent);
            table2.addCell(contact);
            table2.addCell(contactContent);
            table2.addCell(email);
            table2.addCell(emailContent);
//
            PdfPTable table3 = new PdfPTable(1); // 3 columns.
            table3.setSpacingBefore(15f);
            table3.addCell(titleTable4);

            PdfPTable table4 = new PdfPTable(2); // 3 columns.
            table4.setWidths(baseColumn);
            table4.addCell(table4ShopName);
            table4.addCell(table4ShopNameContent);
            table4.addCell(table4BusinessType);
            table4.addCell(table4BusinessTypeContent);
            table4.addCell(table4Endereco);
            table4.addCell(table4EnderecoContent);
            table4.addCell(table4Province);
            table4.addCell(table4ProvinceContent);
            table4.addCell(table4ContactPerson);
            table4.addCell(table4ContactPersonContent);
            table4.addCell(table4DOB);
            table4.addCell(table4DOBContent);
            table4.addCell(table4PhoneEmola);
            table4.addCell(table4PhoneEmolaContent);
            table4.addCell(table4Email);
            table4.addCell(table4EmailContent);
            table4.addCell(table4Nuit);
            table4.addCell(table4NuitContent);

            PdfPTable table5 = new PdfPTable(1); // 3 columns.
            table5.setSpacingBefore(15f);
            table5.addCell(titleTable5);

            PdfPTable table6 = new PdfPTable(2); // 3 columns.
            table6.setWidths(baseColumn);
            table6.addCell(table6NameEmola);
            table6.addCell(table6NameEmolaContent);
            table6.addCell(table6Signature);
            table6.addCell(imageSignEmola);

            PdfPTable table7 = new PdfPTable(1); // 3 columns.
            table7.setSpacingBefore(15f);
            table7.addCell(titleTable7);

            PdfPTable table8 = new PdfPTable(2); // 3 columns.
            table8.setWidths(baseColumn);
            table8.addCell(table8NameEmola);
            table8.addCell(table8NameEmolaContent);
            table8.addCell(table8Signature);
            table8.addCell(imageTable8SignEmola);

            PdfPTable table9 = new PdfPTable(1); // 3 columns.
            table9.setSpacingBefore(15f);
            table9.addCell(titleTable9);

            PdfPTable table10 = new PdfPTable(2); // 3 columns.
            table10.setWidths(baseColumn);
            table10.addCell(table10NameEmola);
            table10.addCell(table10NameEmolaContent);
            table10.addCell(table10Signature);
            table10.addCell(imageTable10SignEmola);

            PdfPTable table11 = new PdfPTable(1); // 3 columns.
            table11.addCell(titleTable11);
            table11.addCell(titleTable111);
            table11.addCell(titleTable112);
            table11.addCell(titleTable113);
            table11.addCell(titleTable114);
            table11.addCell(titleTable115);
            table11.addCell(titleTable116);
            table11.addCell(titleTable117);
            table11.addCell(titleTable118);
            table11.addCell(titleTable119);
            table11.addCell(titleTable1110);
            table11.addCell(titleTable1111);
            table11.addCell(titleTable1112);
            table11.addCell(titleTable1113);
            table11.addCell(titleTable1114);
            table11.addCell(titleTable1115);
            table11.addCell(titleTable1116);
            table11.addCell(titleTable1117);
            table11.addCell(titleTable1118);
            table11.addCell(titleTable1119);
            table11.addCell(titleTable1120);
            table11.addCell(titleTable1121);
            table11.addCell(titleTable1122);
            table11.addCell(titleTable1123);
            table11.addCell(titleTable1124);
            table11.addCell(titleTable1125);
            table11.addCell(titleTable1126);
            table11.addCell(titleTable1127);
            table11.addCell(titleTable1128);
            table11.addCell(titleTable1129);
            table11.addCell(titleTable1130);
            table11.addCell(titleTable1131);
            table11.addCell(titleTable1132);
            table11.addCell(titleTable1133);

            document.add(tableLogo);
            document.add(table0);
            document.add(table1);
            document.add(table2);
            document.add(table3);
            document.add(table4);
            document.add(table5);
            document.add(table6);
            document.add(table7);
            document.add(table8);
            document.add(table9);
            document.add(table10);

            document.newPage();
            document.add(tableLogo);
            document.add(table11);

            document.close();
            output.close();
            FileInputStream fis = new FileInputStream(myFile);
            ftpClient.storeFile(myFile.getName(), fis);
            myFile.delete();
            fis.close();
            myFile.delete();
//            pdfWriter.close();

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (DocumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
        }
    }

    public void createMinutesHandover(RequestChannel contractInfo, String contractNumber, String sysdate,
            String pathSignB, FTPClient ftpClient, String basePath) throws IOException {//sysdate: YYYYMMDD

        Font header = FontFactory.getFont(FontFactory.TIMES, 14, Font.BOLD,
                new CMYKColor(0, 0, 0, 255));
        Font normalFont = FontFactory.getFont(FontFactory.TIMES, 13, Font.NORMAL,
                new CMYKColor(0, 0, 0, 255));
        Font normalFontBold = FontFactory.getFont(FontFactory.TIMES, 13, Font.BOLD,
                new CMYKColor(0, 0, 0, 255));
        Font content = FontFactory.getFont(FontFactory.TIMES, 8, Font.NORMAL, BaseColor.BLACK);
        File myFile = new File(path);
        FileOutputStream output = null;
        try {
            output = new FileOutputStream(myFile);
            // Step 2
            PdfWriter pdfWriter = PdfWriter.getInstance(document, output);
            // Step 3
            document.open();
            // Step 4 Add content
            /* Hien thi tieu de */
            PdfPCell title = new PdfPCell(new Paragraph(ContractConstants.handover_form_1, header));
            title.setBorderColor(BaseColor.WHITE);
            title.setPaddingLeft(5);
            title.setHorizontalAlignment(Element.ALIGN_CENTER);
            title.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell contractNo = new PdfPCell(new Paragraph(String.format(ContractConstants.handover_form_2, contractNumber), normalFont));
            contractNo.setBorderColor(BaseColor.WHITE);
            contractNo.setPaddingLeft(5);
            contractNo.setHorizontalAlignment(Element.ALIGN_CENTER);
            contractNo.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell dateTime = new PdfPCell(new Paragraph(String.format(ContractConstants.handover_form_3, sysdate.substring(6, 8), sysdate.substring(4, 6), sysdate.substring(0, 4)), content));
            dateTime.setBorderColor(BaseColor.WHITE);
            dateTime.setPaddingLeft(5);
            dateTime.setHorizontalAlignment(Element.ALIGN_CENTER);
            dateTime.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPCell tableA = new PdfPCell(new Paragraph(ContractConstants.handover_form_4, normalFontBold));
            tableA.setBorderColor(BaseColor.WHITE);
            tableA.setPaddingLeft(10);
            tableA.setHorizontalAlignment(Element.ALIGN_LEFT);
            tableA.setVerticalAlignment(Element.ALIGN_CENTER);
            tableA.setPaddingBottom(2);

            PdfPCell form5 = new PdfPCell(new Paragraph(ContractConstants.handover_form_5, content));
            form5.setBorderColor(BaseColor.WHITE);
            form5.setPaddingLeft(10);
            form5.setHorizontalAlignment(Element.ALIGN_LEFT);
            form5.setVerticalAlignment(Element.ALIGN_CENTER);
            form5.setPaddingBottom(2);

            PdfPCell form6 = new PdfPCell(new Paragraph(ContractConstants.handover_form_6, content));
            form6.setBorderColor(BaseColor.WHITE);
            form6.setPaddingLeft(10);
            form6.setHorizontalAlignment(Element.ALIGN_LEFT);
            form6.setVerticalAlignment(Element.ALIGN_CENTER);
            form6.setPaddingBottom(2);

            PdfPCell form7 = new PdfPCell(new Paragraph(ContractConstants.handover_form_7, content));
            form7.setBorderColor(BaseColor.WHITE);
            form7.setPaddingLeft(10);
            form7.setHorizontalAlignment(Element.ALIGN_LEFT);
            form7.setVerticalAlignment(Element.ALIGN_CENTER);
            form7.setPaddingBottom(5);

            PdfPCell form8 = new PdfPCell(new Paragraph(String.format(ContractConstants.handover_form_8, contractInfo.getChannelName()), normalFontBold));
            form8.setBorderColor(BaseColor.WHITE);
            form8.setPaddingLeft(10);
            form8.setHorizontalAlignment(Element.ALIGN_LEFT);
            form8.setVerticalAlignment(Element.ALIGN_CENTER);
            form8.setPaddingBottom(2);

            PdfPCell form9 = new PdfPCell(new Paragraph(String.format(ContractConstants.handover_form_9, (contractInfo.getNuitNumber() == null) ? "" : contractInfo.getNuitNumber()), content));
            form9.setBorderColor(BaseColor.WHITE);
            form9.setPaddingLeft(10);
            form9.setHorizontalAlignment(Element.ALIGN_LEFT);
            form9.setVerticalAlignment(Element.ALIGN_CENTER);
            form9.setPaddingBottom(2);

            PdfPCell form10 = new PdfPCell(new Paragraph(ContractConstants.handover_form_10, content));
            form10.setBorderColor(BaseColor.WHITE);
            form10.setPaddingLeft(10);
            form10.setHorizontalAlignment(Element.ALIGN_LEFT);
            form10.setVerticalAlignment(Element.ALIGN_CENTER);
            form10.setPaddingBottom(2);

            PdfPCell form11 = new PdfPCell(new Paragraph(String.format(ContractConstants.handover_form_11, contractInfo.getChannelName()), content));
            form11.setBorderColor(BaseColor.WHITE);
            form11.setPaddingLeft(10);
            form11.setHorizontalAlignment(Element.ALIGN_LEFT);
            form11.setVerticalAlignment(Element.ALIGN_CENTER);
            form11.setPaddingBottom(2);

            PdfPCell form12 = new PdfPCell(new Paragraph(String.format(ContractConstants.handover_form_12, contractInfo.getbINumber()), content));
            form12.setBorderColor(BaseColor.WHITE);
            form12.setPaddingLeft(10);
            form12.setHorizontalAlignment(Element.ALIGN_LEFT);
            form12.setVerticalAlignment(Element.ALIGN_CENTER);
            form12.setPaddingBottom(2);

            PdfPCell form13 = new PdfPCell(new Paragraph(ContractConstants.handover_form_13, content));
            form13.setBorderColor(BaseColor.WHITE);
            form13.setPaddingLeft(10);
            form13.setHorizontalAlignment(Element.ALIGN_LEFT);
            form13.setVerticalAlignment(Element.ALIGN_CENTER);
            form13.setPaddingBottom(2);

            PdfPCell form14 = new PdfPCell(new Paragraph(ContractConstants.handover_form_14, content));
            form14.setBorderColor(BaseColor.WHITE);
            form14.setPaddingLeft(10);
            form14.setHorizontalAlignment(Element.ALIGN_LEFT);
            form14.setVerticalAlignment(Element.ALIGN_CENTER);
            form14.setPaddingBottom(2);

            PdfPCell form15 = new PdfPCell(new Paragraph(ContractConstants.handover_form_15, content));
            form15.setBorderColor(BaseColor.WHITE);
            form15.setPaddingLeft(10);
            form15.setHorizontalAlignment(Element.ALIGN_LEFT);
            form15.setVerticalAlignment(Element.ALIGN_CENTER);
            form15.setPaddingBottom(2);

            PdfPCell form16 = new PdfPCell(new Paragraph(ContractConstants.handover_form_16, content));
            form16.setBorderColor(BaseColor.WHITE);
            form16.setPaddingLeft(10);
            form16.setHorizontalAlignment(Element.ALIGN_LEFT);
            form16.setVerticalAlignment(Element.ALIGN_CENTER);
            form16.setPaddingBottom(2);


            PdfPCell table7No = new PdfPCell(new Paragraph(ContractConstants.handover_form_17, content));
            table7No.setBorderColor(BaseColor.BLACK);
            table7No.setHorizontalAlignment(Element.ALIGN_CENTER);
            table7No.setVerticalAlignment(Element.ALIGN_CENTER);
            table7No.setPaddingBottom(5);

            PdfPCell table7Form18 = new PdfPCell(new Paragraph(ContractConstants.handover_form_18, content));
            table7Form18.setBorderColor(BaseColor.BLACK);
            table7Form18.setHorizontalAlignment(Element.ALIGN_CENTER);
            table7Form18.setVerticalAlignment(Element.ALIGN_CENTER);
            table7Form18.setPaddingBottom(5);

            PdfPCell table7Form19 = new PdfPCell(new Paragraph(ContractConstants.handover_form_19, content));
            table7Form19.setBorderColor(BaseColor.BLACK);
            table7Form19.setHorizontalAlignment(Element.ALIGN_CENTER);
            table7Form19.setVerticalAlignment(Element.ALIGN_CENTER);
            table7Form19.setPaddingBottom(5);

            PdfPCell table7Form20 = new PdfPCell(new Paragraph(ContractConstants.handover_form_20, content));
            table7Form20.setBorderColor(BaseColor.BLACK);
            table7Form20.setHorizontalAlignment(Element.ALIGN_CENTER);
            table7Form20.setVerticalAlignment(Element.ALIGN_CENTER);
            table7Form20.setPaddingBottom(5);

            PdfPCell table7Form21 = new PdfPCell(new Paragraph(ContractConstants.handover_form_21, content));
            table7Form21.setBorderColor(BaseColor.BLACK);
            table7Form21.setHorizontalAlignment(Element.ALIGN_CENTER);
            table7Form21.setVerticalAlignment(Element.ALIGN_CENTER);
            table7Form21.setPaddingBottom(5);

            String[] arrEquipment = contractInfo.getEquipmentInfo().split("\\|");

            PdfPCell equipment1 = new PdfPCell(new Paragraph("1", content));
            equipment1.setBorderColor(BaseColor.BLACK);
            equipment1.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment1.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment1.setPaddingBottom(5);

            PdfPCell equipment2 = new PdfPCell(new Paragraph(ContractConstants.handover_form_23, content));
            equipment2.setBorderColor(BaseColor.BLACK);
            equipment2.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment2.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment2.setPaddingBottom(5);

            PdfPCell equipment3 = new PdfPCell(new Paragraph("", content));
            equipment3.setBorderColor(BaseColor.BLACK);
            equipment3.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment3.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment3.setPaddingBottom(5);

            PdfPCell equipment4 = new PdfPCell(new Paragraph("", content));
            equipment4.setBorderColor(BaseColor.BLACK);
            equipment4.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment4.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment4.setPaddingBottom(5);

            for (int i = 0; i < arrEquipment.length; i++) {
                if (arrEquipment[i].contains(ContractConstants.handover_form_23)) {
                    String[] tmpEquipment = arrEquipment[i].split(":");
                    equipment3 = new PdfPCell(new Paragraph(tmpEquipment[1], content));
                    equipment3.setBorderColor(BaseColor.BLACK);
                    equipment3.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment3.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment3.setPaddingBottom(5);

                    equipment4 = new PdfPCell(new Paragraph(tmpEquipment[2].replace(".", "\n"), content));
                    equipment4.setBorderColor(BaseColor.BLACK);
                    equipment4.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment4.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment4.setPaddingBottom(5);
                    break;
                }
            }

            PdfPCell equipment5 = new PdfPCell(new Paragraph(ContractConstants.handover_form_30, content));
            equipment5.setBorderColor(BaseColor.BLACK);
            equipment5.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment5.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment5.setPaddingBottom(5);

            PdfPCell equipment6 = new PdfPCell(new Paragraph("2", content));
            equipment6.setBorderColor(BaseColor.BLACK);
            equipment6.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment6.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment6.setPaddingBottom(5);

            PdfPCell equipment7 = new PdfPCell(new Paragraph(ContractConstants.handover_form_24, content));
            equipment7.setBorderColor(BaseColor.BLACK);
            equipment7.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment7.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment7.setPaddingBottom(5);

            PdfPCell equipment8 = new PdfPCell(new Paragraph("", content));
            equipment8.setBorderColor(BaseColor.BLACK);
            equipment8.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment8.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment8.setPaddingBottom(5);

            PdfPCell equipment9 = new PdfPCell(new Paragraph("", content));
            equipment9.setBorderColor(BaseColor.BLACK);
            equipment9.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment9.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment9.setPaddingBottom(5);

            for (int i = 0; i < arrEquipment.length; i++) {
                if (arrEquipment[i].contains(ContractConstants.handover_form_24)) {
                    String[] tmpEquipment = arrEquipment[i].split(":");
                    equipment8 = new PdfPCell(new Paragraph(tmpEquipment[1], content));
                    equipment8.setBorderColor(BaseColor.BLACK);
                    equipment8.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment8.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment8.setPaddingBottom(5);

                    equipment9 = new PdfPCell(new Paragraph(tmpEquipment[2].replace(".", "\n"), content));
                    equipment9.setBorderColor(BaseColor.BLACK);
                    equipment9.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment9.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment9.setPaddingBottom(5);
                    break;
                }
            }




            PdfPCell equipment10 = new PdfPCell(new Paragraph(ContractConstants.handover_form_30, content));
            equipment10.setBorderColor(BaseColor.BLACK);
            equipment10.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment10.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment10.setPaddingBottom(5);

            PdfPCell equipment11 = new PdfPCell(new Paragraph("3", content));
            equipment11.setBorderColor(BaseColor.BLACK);
            equipment11.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment11.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment11.setPaddingBottom(5);

            PdfPCell equipment12 = new PdfPCell(new Paragraph(ContractConstants.handover_form_25, content));
            equipment12.setBorderColor(BaseColor.BLACK);
            equipment12.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment12.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment12.setPaddingBottom(5);

            PdfPCell equipment13 = new PdfPCell(new Paragraph("", content));
            equipment13.setBorderColor(BaseColor.BLACK);
            equipment13.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment13.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment13.setPaddingBottom(5);

            PdfPCell equipment14 = new PdfPCell(new Paragraph("", content));
            equipment14.setBorderColor(BaseColor.BLACK);
            equipment14.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment14.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment14.setPaddingBottom(5);

            for (int i = 0; i < arrEquipment.length; i++) {
                if (arrEquipment[i].contains(ContractConstants.handover_form_25)) {
                    String[] tmpEquipment = arrEquipment[i].split(":");
                    equipment13 = new PdfPCell(new Paragraph(tmpEquipment[1], content));
                    equipment13.setBorderColor(BaseColor.BLACK);
                    equipment13.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment13.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment13.setPaddingBottom(5);

                    equipment14 = new PdfPCell(new Paragraph(tmpEquipment[2].replace(".", "\n"), content));
                    equipment14.setBorderColor(BaseColor.BLACK);
                    equipment14.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment14.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment14.setPaddingBottom(5);
                    break;
                }
            }

            PdfPCell equipment15 = new PdfPCell(new Paragraph(ContractConstants.handover_form_30, content));
            equipment15.setBorderColor(BaseColor.BLACK);
            equipment15.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment15.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment15.setPaddingBottom(5);


            PdfPCell equipment16 = new PdfPCell(new Paragraph("4", content));
            equipment16.setBorderColor(BaseColor.BLACK);
            equipment16.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment16.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment16.setPaddingBottom(5);

            PdfPCell equipment17 = new PdfPCell(new Paragraph(ContractConstants.handover_form_26, content));
            equipment17.setBorderColor(BaseColor.BLACK);
            equipment17.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment17.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment17.setPaddingBottom(5);

            PdfPCell equipment18 = new PdfPCell(new Paragraph("", content));
            equipment18.setBorderColor(BaseColor.BLACK);
            equipment18.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment18.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment18.setPaddingBottom(5);

            PdfPCell equipment19 = new PdfPCell(new Paragraph("", content));
            equipment19.setBorderColor(BaseColor.BLACK);
            equipment19.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment19.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment19.setPaddingBottom(5);

            for (int i = 0; i < arrEquipment.length; i++) {
                if (arrEquipment[i].contains(ContractConstants.handover_form_26)) {
                    String[] tmpEquipment = arrEquipment[i].split(":");
                    equipment18 = new PdfPCell(new Paragraph(tmpEquipment[1], content));
                    equipment18.setBorderColor(BaseColor.BLACK);
                    equipment18.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment18.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment18.setPaddingBottom(5);

                    equipment19 = new PdfPCell(new Paragraph(tmpEquipment[2].replace(".", "\n"), content));
                    equipment19.setBorderColor(BaseColor.BLACK);
                    equipment19.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment19.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment19.setPaddingBottom(5);
                    break;
                }
            }
            PdfPCell equipment20 = new PdfPCell(new Paragraph(ContractConstants.handover_form_30, content));
            equipment20.setBorderColor(BaseColor.BLACK);
            equipment20.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment20.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment20.setPaddingBottom(5);

            PdfPCell equipment21 = new PdfPCell(new Paragraph("5", content));
            equipment21.setBorderColor(BaseColor.BLACK);
            equipment21.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment21.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment21.setPaddingBottom(5);

            PdfPCell equipment22 = new PdfPCell(new Paragraph(ContractConstants.handover_form_27, content));
            equipment22.setBorderColor(BaseColor.BLACK);
            equipment22.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment22.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment22.setPaddingBottom(5);

            PdfPCell equipment23 = new PdfPCell(new Paragraph("", content));
            equipment23.setBorderColor(BaseColor.BLACK);
            equipment23.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment23.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment23.setPaddingBottom(5);

            PdfPCell equipment24 = new PdfPCell(new Paragraph("", content));
            equipment24.setBorderColor(BaseColor.BLACK);
            equipment24.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment24.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment24.setPaddingBottom(5);

            for (int i = 0; i < arrEquipment.length; i++) {
                if (arrEquipment[i].contains(ContractConstants.handover_form_27)) {
                    String[] tmpEquipment = arrEquipment[i].split(":");
                    equipment23 = new PdfPCell(new Paragraph(tmpEquipment[1], content));
                    equipment23.setBorderColor(BaseColor.BLACK);
                    equipment23.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment23.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment23.setPaddingBottom(5);

                    equipment24 = new PdfPCell(new Paragraph(tmpEquipment[2].replace(".", "\n"), content));
                    equipment24.setBorderColor(BaseColor.BLACK);
                    equipment24.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment24.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment24.setPaddingBottom(5);
                    break;
                }
            }
            PdfPCell equipment25 = new PdfPCell(new Paragraph(ContractConstants.handover_form_30, content));
            equipment25.setBorderColor(BaseColor.BLACK);
            equipment25.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment25.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment25.setPaddingBottom(5);

            PdfPCell equipment26 = new PdfPCell(new Paragraph("6", content));
            equipment26.setBorderColor(BaseColor.BLACK);
            equipment26.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment26.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment26.setPaddingBottom(5);

            PdfPCell equipment27 = new PdfPCell(new Paragraph(ContractConstants.handover_form_28, content));
            equipment27.setBorderColor(BaseColor.BLACK);
            equipment27.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment27.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment27.setPaddingBottom(5);

            PdfPCell equipment28 = new PdfPCell(new Paragraph("", content));
            equipment28.setBorderColor(BaseColor.BLACK);
            equipment28.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment28.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment28.setPaddingBottom(5);

            PdfPCell equipment29 = new PdfPCell(new Paragraph("", content));
            equipment29.setBorderColor(BaseColor.BLACK);
            equipment29.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment29.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment29.setPaddingBottom(5);

            for (int i = 0; i < arrEquipment.length; i++) {
                if (arrEquipment[i].contains(ContractConstants.handover_form_28)) {
                    String[] tmpEquipment = arrEquipment[i].split(":");
                    equipment28 = new PdfPCell(new Paragraph(tmpEquipment[1], content));
                    equipment28.setBorderColor(BaseColor.BLACK);
                    equipment28.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment28.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment28.setPaddingBottom(5);

                    equipment29 = new PdfPCell(new Paragraph(tmpEquipment[2].replace(".", "\n"), content));
                    equipment29.setBorderColor(BaseColor.BLACK);
                    equipment29.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment29.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment29.setPaddingBottom(5);
                    break;
                }
            }
            PdfPCell equipment30 = new PdfPCell(new Paragraph(ContractConstants.handover_form_30, content));
            equipment30.setBorderColor(BaseColor.BLACK);
            equipment30.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment30.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment30.setPaddingBottom(5);

            PdfPCell equipment31 = new PdfPCell(new Paragraph("7", content));
            equipment31.setBorderColor(BaseColor.BLACK);
            equipment31.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment31.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment31.setPaddingBottom(5);

            PdfPCell equipment32 = new PdfPCell(new Paragraph(ContractConstants.handover_form_29, content));
            equipment32.setBorderColor(BaseColor.BLACK);
            equipment32.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment32.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment32.setPaddingBottom(5);

            PdfPCell equipment33 = new PdfPCell(new Paragraph("", content));
            equipment33.setBorderColor(BaseColor.BLACK);
            equipment33.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment33.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment33.setPaddingBottom(5);

            PdfPCell equipment34 = new PdfPCell(new Paragraph("", content));
            equipment34.setBorderColor(BaseColor.BLACK);
            equipment34.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment34.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment34.setPaddingBottom(5);

            for (int i = 0; i < arrEquipment.length; i++) {
                if (arrEquipment[i].contains(ContractConstants.handover_form_29)) {
                    String[] tmpEquipment = arrEquipment[i].split(":");
                    equipment33 = new PdfPCell(new Paragraph(tmpEquipment[1], content));
                    equipment33.setBorderColor(BaseColor.BLACK);
                    equipment33.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment33.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment33.setPaddingBottom(5);

                    equipment34 = new PdfPCell(new Paragraph(tmpEquipment[2].replace(".", "\n"), content));
                    equipment34.setBorderColor(BaseColor.BLACK);
                    equipment34.setHorizontalAlignment(Element.ALIGN_CENTER);
                    equipment34.setVerticalAlignment(Element.ALIGN_CENTER);
                    equipment34.setPaddingBottom(5);
                    break;
                }
            }
            PdfPCell equipment35 = new PdfPCell(new Paragraph(ContractConstants.handover_form_30, content));
            equipment35.setBorderColor(BaseColor.BLACK);
            equipment35.setHorizontalAlignment(Element.ALIGN_CENTER);
            equipment35.setVerticalAlignment(Element.ALIGN_CENTER);
            equipment35.setPaddingBottom(5);


            PdfPCell form22 = new PdfPCell(new Paragraph(ContractConstants.handover_form_22, content));
            form22.setBorderColor(BaseColor.WHITE);
            form22.setPaddingLeft(10);
            form22.setHorizontalAlignment(Element.ALIGN_LEFT);
            form22.setVerticalAlignment(Element.ALIGN_CENTER);
            form22.setPaddingBottom(2);

            PdfPCell signA = new PdfPCell(new Paragraph(ContractConstants.registration_form_41, normalFontBold));
            signA.setBorderColor(BaseColor.WHITE);
            signA.setPaddingLeft(10);
            signA.setHorizontalAlignment(Element.ALIGN_CENTER);
            signA.setVerticalAlignment(Element.ALIGN_CENTER);
            signA.setPaddingBottom(5);

            PdfPCell signB = new PdfPCell(new Paragraph(ContractConstants.registration_form_42, normalFontBold));
            signB.setBorderColor(BaseColor.WHITE);
            signB.setPaddingLeft(10);
            signB.setPaddingBottom(5);
            signB.setHorizontalAlignment(Element.ALIGN_CENTER);
            signB.setVerticalAlignment(Element.ALIGN_CENTER);

            Image imageA = Image.getInstance(getBytes(basePath + "/etc/imgSignPartA.png"));
            imageA.scaleAbsolute(60, 40);
            PdfPCell imageSignA = new PdfPCell(imageA);
            imageSignA.setBorderColor(BaseColor.WHITE);
            imageSignA.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageSignA.setVerticalAlignment(Element.ALIGN_MIDDLE);

            Image imageB = Image.getInstance(getBytes(pathSignB));
            imageB.scaleAbsolute(120, 90);
            PdfPCell imageSignB = new PdfPCell(imageB);
            imageSignB.setBorderColor(BaseColor.WHITE);
            imageSignB.setHorizontalAlignment(Element.ALIGN_CENTER);
            imageSignB.setVerticalAlignment(Element.ALIGN_MIDDLE);

            PdfPTable table0 = new PdfPTable(1); // 3 columns.
            table0.addCell(title);

            PdfPTable table1 = new PdfPTable(1); // 3 columns.
            table1.addCell(contractNo);

            PdfPTable table2 = new PdfPTable(1); // 3 columns.
            table2.addCell(dateTime);

            PdfPTable table6 = new PdfPTable(1); // 3 columns.
            table6.addCell(tableA);
            table6.addCell(form5);
            table6.addCell(form6);
            table6.addCell(form7);
            table6.addCell(form8);
            table6.addCell(form9);
            table6.addCell(form10);
            table6.addCell(form11);
            table6.addCell(form12);
            table6.addCell(form13);
            table6.addCell(form14);
            table6.addCell(form15);
            table6.addCell(form16);

            PdfPTable table7 = new PdfPTable(5); // 3 columns.
            table7.setWidths(new int[]{1, 2, 2, 3, 1});
            table7.setSpacingBefore(5f);
            table7.addCell(table7No);
            table7.addCell(table7Form18);
            table7.addCell(table7Form19);
            table7.addCell(table7Form20);
            table7.addCell(table7Form21);

            table7.addCell(equipment1);
            table7.addCell(equipment2);
            table7.addCell(equipment3);
            table7.addCell(equipment4);
            table7.addCell(equipment5);
            table7.addCell(equipment6);
            table7.addCell(equipment7);
            table7.addCell(equipment8);
            table7.addCell(equipment9);
            table7.addCell(equipment10);
            table7.addCell(equipment11);
            table7.addCell(equipment12);
            table7.addCell(equipment13);
            table7.addCell(equipment14);
            table7.addCell(equipment15);
            table7.addCell(equipment16);
            table7.addCell(equipment17);
            table7.addCell(equipment18);
            table7.addCell(equipment19);
            table7.addCell(equipment20);
            table7.addCell(equipment21);
            table7.addCell(equipment22);
            table7.addCell(equipment23);
            table7.addCell(equipment24);
            table7.addCell(equipment25);
            table7.addCell(equipment26);
            table7.addCell(equipment27);
            table7.addCell(equipment28);
            table7.addCell(equipment29);
            table7.addCell(equipment30);
            table7.addCell(equipment31);
            table7.addCell(equipment32);
            table7.addCell(equipment33);
            table7.addCell(equipment34);
            table7.addCell(equipment35);

            PdfPTable table8 = new PdfPTable(1); // 3 columns.
            table8.addCell(form22);

            PdfPTable table10 = new PdfPTable(2); // 3 columns.
            table10.setSpacingBefore(15f); // Space before table
            table10.setSpacingAfter(15f);
            table10.addCell(signA);
            table10.addCell(signB);
            table10.addCell(imageSignA);
            table10.addCell(imageSignB);


            document.add(table0);
            document.add(table1);
            document.add(table2);
            document.add(table6);
            document.add(table7);
            document.add(table8);
            document.add(table10);

            document.close();
            output.close();
            FileInputStream fis = new FileInputStream(myFile);
            ftpClient.storeFile(myFile.getName(), fis);
            fis.close();
            myFile.delete();
//            pdfWriter.close();

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (DocumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
        }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public byte[] getBytes(String fileName) throws FileNotFoundException, IOException {
        byte[] buffer = new byte[4096];
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int bytes = 0;
        while ((bytes = bis.read(buffer, 0, buffer.length)) > 0) {
            baos.write(buffer, 0, bytes);
        }
        baos.close();
        bis.close();
        return baos.toByteArray();
    }
}
