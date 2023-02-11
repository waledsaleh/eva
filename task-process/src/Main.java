import factory.FactoryProcess;
import template.AbstractTemplate;

public class Main {

    public static void main(String[] args) {
        AbstractTemplate template = FactoryProcess.createTemplate();
        template.steps("local", "eva-task", "dataset", "dataset/output");
    }

}
