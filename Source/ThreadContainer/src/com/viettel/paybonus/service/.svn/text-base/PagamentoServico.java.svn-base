package com.viettel.paybonus.service;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PagamentoServico {

    public PagamentoServico() {
    }

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        Date begin = sdf.parse("21032019000000000");
        Date end = sdf.parse("26042019000000000");
        Date now = new Date();
        if (now.before(end) && now.after(begin)) {
            System.out.println("Obrigado por participar do Programa da Movitel de Apoio as Vitimas do Ciclone IDAI. A sua doacao: 50 MT. Para detalhes acesse o nosso Website ou Pagina Facebook");
        } else {
            System.out.println("Not yet ");
        }
        SimpleDateFormat sdf3 = new SimpleDateFormat("MMyyyy");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        boolean bl = sdf.parse("20190307100149").compareTo(new Date()) > 0;
        System.out.println("test1 " + bl);
        long test = 1024 * 1024 * Long.valueOf("2048");
        System.out.println("test " + test);
        String entidade = "20005";
        String referenciaSemCheckDigito = "002841809";
        String montanteComDuasCasasDecimais = "18032";

        getCheckDigitoReferencia(entidade, referenciaSemCheckDigito, montanteComDuasCasasDecimais);
    }

    public static void getCheckDigitoReferencia(String entidade, String referenciaSemCheckDigito, String montanteComDuasCasasDecimais) {
        StringBuffer digitos = new StringBuffer();
        digitos.append(entidade);
        digitos.append(referenciaSemCheckDigito);
        digitos.append(montanteComDuasCasasDecimais);

        int s = 0;
        int p = 0;
        for (int i = 0; i < digitos.length(); i++) {
            s = Integer.parseInt(String.valueOf(digitos.charAt(i))) + p;
            p = s * 10 % 97;
        }
        p = p * 10 % 97;
        int checkDigitoCalculado = 98 - p;

        int checkDigito = Integer.parseInt(referenciaSemCheckDigito);

        System.out.println("CheckDigito Calculado: " + checkDigitoCalculado);
        System.out.println("Referência com CheckDigito: " + (referenciaSemCheckDigito + "" + checkDigitoCalculado));
//                0600010604201800002
//10038089011100000000007000000000000000000000060420181532390000000                          
//9000000100000000007000000000000000000000

    }
}
