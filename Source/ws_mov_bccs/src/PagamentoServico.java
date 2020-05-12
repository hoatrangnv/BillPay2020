
import java.util.Random;

public class PagamentoServico {

    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        sb.setLength(0);
        for (int i = 0; i < 1000; i++) {
            Random rand = new Random();
            int r = rand.nextInt(26119670 - 3139741 + 1) + 3139741;
            sb.append(r).append(",");            
        }
        System.out.println(sb.toString().substring(0, sb.length()-1));
//        String entidade = "86871";
//        String referenciaSemCheckDigito = "011876608";
//        String montanteComDuasCasasDecimais = "";
//
//        getCheckDigitoReferencia(entidade, referenciaSemCheckDigito, montanteComDuasCasasDecimais);
    }

    public static void getCheckDigitoReferencia(String entidade, String referenciaSemCheckDigito, String montanteComDuasCasasDecimais) {
        StringBuffer digitos = new StringBuffer();
        digitos.append(entidade);
        digitos.append(referenciaSemCheckDigito);
        digitos.append(montanteComDuasCasasDecimais);

        int s = 0;
        int p = 0;
        System.out.println("EntityInvoicenumberMonthAmount: " + digitos);
        for (int i = 1; i <= digitos.length(); i++) {
            s = Integer.parseInt(String.valueOf(digitos.charAt(i - 1))) + p;
            System.out.println("s" + i + ": " + s);
            p = s * 10 % 97;
            System.out.println("p" + i + ": " + p);
        }
        p = p * 10 % 97;
        System.out.println("Pn" + ": " + p);
        int checkDigitoCalculado = 98 - p;
        int checkDigito = Integer.parseInt(referenciaSemCheckDigito);

        System.out.println("CD: " + checkDigitoCalculado);
        System.out.println("Reference: " + (referenciaSemCheckDigito + "" + checkDigitoCalculado));
    }
}
