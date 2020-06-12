import org.apache.spark.sql.execution.vectorized.ColumnVector;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;

public class Utils {
    private static void mosaic(String input, String output, int mosaicSize) throws Exception {
        new File(input.substring(0, input.lastIndexOf("/"))).mkdirs();
        new File(output.substring(0, output.lastIndexOf("/"))).mkdirs();
        File file = new File(input);
        if (!file.isFile()) {
            throw new Exception(file + " 不是一个图片文件!");
        }
        BufferedImage bi = ImageIO.read(file); // 读取该图片
        BufferedImage spinImage = new BufferedImage(bi.getWidth(),
                bi.getHeight(), bi.TYPE_INT_RGB);
        if (bi.getWidth() < mosaicSize || bi.getHeight() < mosaicSize || mosaicSize <= 0) {
            throw new Exception("马赛克格尺寸太大或太小");
        }

        int xcount = 0; // 方向绘制个数
        int ycount = 0; // y方向绘制个数
        if (bi.getWidth() % mosaicSize == 0) {
            xcount = bi.getWidth() / mosaicSize;
        } else {
            xcount = bi.getWidth() / mosaicSize + 1;
        }
        if (bi.getHeight() % mosaicSize == 0) {
            ycount = bi.getHeight() / mosaicSize;
        } else {
            ycount = bi.getHeight() / mosaicSize + 1;
        }
        int x = 0;   //坐标
        int y = 0;
        // 绘制马赛克(绘制矩形并填充颜色)
        Graphics gs = spinImage.getGraphics();
        for (int i = 0; i < xcount; i++) {
            for (int j = 0; j < ycount; j++) {
                //马赛克矩形格大小
                int mwidth = mosaicSize;
                int mheight = mosaicSize;
                if (i == xcount - 1) {   //横向最后一个比较特殊，可能不够一个size
                    mwidth = bi.getWidth() - x;
                }
                if (j == ycount - 1) {  //同理
                    mheight = bi.getHeight() - y;
                }
                // 矩形颜色取中心像素点RGB值
                int centerX = x;
                int centerY = y;
                if (mwidth % 2 == 0) {
                    centerX += mwidth / 2;
                } else {
                    centerX += (mwidth - 1) / 2;
                }
                if (mheight % 2 == 0) {
                    centerY += mheight / 2;
                } else {
                    centerY += (mheight - 1) / 2;
                }
                Color color = new Color(bi.getRGB(centerX, centerY));
                gs.setColor(color);
                gs.fillRect(x, y, mwidth, mheight);
                y = y + mosaicSize;// 计算下一个矩形的y坐标
            }
            y = 0;// 还原y坐标
            x = x + mosaicSize;// 计算x坐标
        }
        gs.dispose();
        File sf = new File(output);
        ImageIO.write(spinImage, output.substring(output.lastIndexOf(".") + 1), sf);
    }

    private static String getImageStr(String imgPath) {
        String imgFile = imgPath;// 待处理的图片
        InputStream in = null;
        byte[] data = null;
        String encode = null; // 返回Base64编码过的字节数组字符串
        // 对字节数组Base64编码
        BASE64Encoder encoder = new BASE64Encoder();
        try {
            // 读取图片字节数组
            new File(imgFile.substring(0, imgFile.lastIndexOf("/"))).mkdirs();
            in = new FileInputStream(imgFile);
            data = new byte[in.available()];
            in.read(data);
            encode = encoder.encode(data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return encode;
    }

    public static boolean generateImage(String imgData, String imgFilePath) throws IOException {
        if (imgData == null) // 图像数据为空
            return false;
        BASE64Decoder decoder = new BASE64Decoder();
        OutputStream out = null;
        try {
            new File(imgFilePath.substring(0, imgFilePath.lastIndexOf("/"))).mkdirs();
            out = new FileOutputStream(imgFilePath);
            // Base64解码
            byte[] b = decoder.decodeBuffer(imgData);
            for (int i = 0; i < b.length; ++i) {
                if (b[i] < 0) {// 调整异常数据
                    b[i] += 256;
                }
            }
            out.write(b);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            assert out != null;
            out.flush();
            out.close();
            return true;
        }
    }

    public static String makePicMosaic(String inputBase64) throws Exception {
        //先保存成图片
        String picName = "tmp/" + System.nanoTime() + ".png";
        generateImage(inputBase64, picName);
        String outputPicName = "tmp/" + System.nanoTime() + ".png";
        mosaic(picName, outputPicName, 10);
        return getImageStr(outputPicName).replaceAll("\r\n", "");
    }

    public static void tuomin(String idCard){
        String card="****"+idCard.substring(4,13)+"***";
        System.out.println(card);
    }
}
