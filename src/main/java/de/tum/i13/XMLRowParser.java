package de.tum.i13;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class XMLRowParser {

    public static ArrayList<byte[]> getNodes(String fileName) {
        ArrayList<byte[]> nodes = new ArrayList<>();

        try {
            File xmlFile = new File(fileName);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document document = dBuilder.parse(xmlFile);

            document.getDocumentElement().normalize();

            NodeList nodeList = document.getElementsByTagName("row");

            for (int i = 1; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);

                OutputFormat outputFormat = new OutputFormat();
                outputFormat.setOmitXMLDeclaration(true);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                XMLSerializer ser = new XMLSerializer(baos, outputFormat);
                ser.serialize(node);

                nodes.add(baos.toByteArray());
            }

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return nodes;
    }
}