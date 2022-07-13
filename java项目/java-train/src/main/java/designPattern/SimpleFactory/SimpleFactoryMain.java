package designPattern.SimpleFactory;

public class SimpleFactoryMain {
    public static void main(String[] args) {
        SimpleFactory factory = new SimpleFactory();
        double numA = 100;
        double numB = 100.2;
        Operation opt = factory.createOperation("+");
        double result = opt.getResult(numA, numB);
        System.out.println(result);
    }
}
