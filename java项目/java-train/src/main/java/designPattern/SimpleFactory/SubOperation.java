package designPattern.SimpleFactory;

public class SubOperation implements Operation {

    @Override
    public double getResult(double numberA, double numberB) {
        return numberA-numberB;
    }
}
