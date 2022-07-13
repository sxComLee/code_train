package designPattern.SimpleFactory;

public class MulOperation implements Operation {

    @Override
    public double getResult(double numberA, double numberB) {
        return numberA*numberB;
    }
}
