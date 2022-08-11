package test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description:
 *
 * @author lij
 * @date 2022-07-25 15:48
 */
public class RegexTest {

    /**
     * 判断字符串是否含有Emoji表情
     **/
    private static boolean isHasEmoji(String reviewerName) {
        Pattern pattern = Pattern.compile("(?:[\uD83C\uDF00-\uD83D\uDDFF]" + // 杂项符号及图形
                "|[\uD83E\uDD00-\uD83E\uDDFF]" + // 增补符号及图形
                "|[\uD83D\uDE00-\uD83D\uDE4F]" + // 表情符号
                "|[\uD83D\uDE80-\uD83D\uDEFF]" + // 交通及地图符号
                "|[\u2600-\u26FF]\uFE0F?" + // 杂项符号
                "|[\u2700-\u27BF]\uFE0F?" + // 装饰符号
                "|\u24C2\uFE0F?" + // 封闭式字母数字符号
                "|[\uD83C\uDDE6-\uD83C\uDDFF]{1,2}" + // 封闭式字母数字补充符号-区域指示符号
                "|[\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91-\uD83C\uDD9A]\uFE0F?" + // 其他封闭式字母数字补充emoji符号
                "|[\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3" + //  键帽符号
                "|[\u2194-\u2199\u21A9-\u21AA]\uFE0F?" + // 箭头符号
                "|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?" + // 杂项符号及箭头
                "|[\u2934\u2935]\uFE0F?" + // 补充箭头符号
                "|[\u3030\u303D]\uFE0F?" + // CJK 符号和标点
                "|[\u3297\u3299]\uFE0F?" + //  封闭式 CJK 字母和月份符号
                "|[\uD83C\uDE01\uD83C\uDE02\uD83C\uDE1A\uD83C\uDE2F\uD83C\uDE32-\uD83C\uDE3A\uD83C\uDE50\uD83C\uDE51]\uFE0F?" + // 封闭式表意文字补充符号
                "|[\u203C\u2049]\uFE0F?" + // 一般标点
                "|[\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE]\uFE0F?" + // 几何图形
                "|[\u00A9\u00AE]\uFE0F?" + // 拉丁文补充符号
                "|[\u2122\u2139]\uFE0F?" + // 字母符号
                "|\uD83C\uDC04\uFE0F?" + // 麻将牌
                "|\uD83C\uDCCF\uFE0F?" + // 扑克牌
                "|[\u231A\u231B\u2328\u23CF\u23E9-\u23F3\u23F8-\u23FA]\uFE0F?)"); // 杂项技术符号
        Matcher matcher = pattern.matcher(reviewerName);
        return matcher.find();
    }

    public static void main(String[] args) {
        String emoji = "\uD83D\uDC4D\uD83D\uDC4D\uD83D\uDC4D\uD83C\uDF39\uD83C\uDF39\uD83C\uDF39";

        System.out.println(isHasEmoji(emoji));
    }

}
