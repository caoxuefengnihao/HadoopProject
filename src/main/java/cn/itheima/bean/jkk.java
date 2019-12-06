package cn.itheima.bean;

public class jkk {
    public static void main(String[] args) {


        String string = "lkh72aj4nno\tthepeceptionis\t653\tentertainment\t424\t13021\t4.34\t1305\t774\tdida-50kyf\tnxtdlno";
        //String string = "sdnkmu82t68\twoody911f\t630\tperson & blogs\t186\t10181\t3.459\t494\t257\trinbgpjuks";
        String[] split = string.split("\t");
        StringBuilder sb = new StringBuilder();
        /**
         * 数据清洗的逻辑
         * 首先  我要遍历这个切分开的数组
         */
        if (split.length<8){return;}
        for (int i = 0; i < split.length; i++) {
            if (i<9){
                if (i == 3){
                    sb.append(split[i].replaceAll(" ","")+"   ");
                }else{
                        sb.append(split[i]+"    ");
                }
            }else {
                if (i == split.length-1){
                    sb.append(split[i]);
                }else {
                    sb.append(split[i]+"&");
                }
            }
        }
        System.out.println(sb.toString());
    }
}
