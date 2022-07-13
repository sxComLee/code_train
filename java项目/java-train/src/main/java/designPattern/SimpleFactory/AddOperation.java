package designPattern.SimpleFactory;

public class AddOperation implements Operation {

    @Override
    public double getResult(double numberA, double numberB) {
        return numberA+numberB;
    }
}
