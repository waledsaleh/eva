package factory;

import template.AbstractTemplate;
import template.ProcessImpl;

public class FactoryProcess {

    /*This is static factory method to create template process*/
    public static AbstractTemplate createTemplate(){
        return new ProcessImpl();
    }
}
