package designPattern.SimpleFactory;

public class SimpleFactory {
    public Operation createOperation(String operation){
        Operation opt = null;
        switch (operation){
            case "+":
                opt = new AddOperation();
                break;
            case "-":
                opt = new SubOperation();
            case "*":
                opt = new MulOperation();
            case "/":
                opt = new SubOperation();
        }
        return opt;
    }
}
