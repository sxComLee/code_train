package designPattern.SimpleFactory;

public class DivOperation implements Operation {

    @Override
    public double getResult(double numberA, double numberB) {
        return numberA/numberB;
    }
}
