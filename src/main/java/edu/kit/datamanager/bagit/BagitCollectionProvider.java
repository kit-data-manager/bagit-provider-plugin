/*
 * Copyright 2019 Karlsruhe Institute of Technology.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.kit.datamanager.bagit;

import org.springframework.stereotype.Component;
import edu.kit.datamanager.exceptions.CustomInternalServerError;
import org.springframework.web.server.UnsupportedMediaTypeStatusException;
import edu.kit.datamanager.service.IContentCollectionProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import edu.kit.datamanager.entities.CollectionElement;
import edu.kit.datamanager.entities.repo.DataResource;
import edu.kit.datamanager.util.ZipUtils;
import edu.kit.datamanager.util.xml.DataCiteMapper;
import edu.kit.datamanager.util.xml.DublinCoreMapper;
import gov.loc.repository.bagit.domain.FetchItem;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import org.apache.commons.io.FileUtils;
import org.datacite.schema.kernel_4.Resource;
import org.purl.dc.elements._1.ElementContainer;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author jejkal
 */

@Component
public class BagitCollectionProvider implements IContentCollectionProvider{

  private final static Logger LOGGER = LoggerFactory.getLogger(BagitCollectionProvider.class);

  public final static MediaType BAGIT_MEDIA_TYPE = MediaType.parseMediaType("application/vnd.datamanager.bagit+zip");

  @Override
  public void provide(List<CollectionElement> collection, MediaType mediaType, HttpServletResponse response){
    if(!BAGIT_MEDIA_TYPE.toString().equals(mediaType.toString())){
      LOGGER.error("Unsupported media type {} received. Throwing HTTP 415 (UNSUPPORTED_MEDIA_TYPE).", mediaType);
      throw new UnsupportedMediaTypeStatusException(mediaType, Arrays.asList(getSupportedMediaTypes()));
    }

    LOGGER.trace("Checking received collection for missing/invalid elements.");
    for(CollectionElement element : collection){
      Path path = Paths.get(element.getContentUri());
      if(!Files.exists(path) || !Files.isReadable(path)){
        LOGGER.error("Failed to locate/read file {} at relative path {} with URI {}. Aborting packaging operation.", element.getContentUri(), element.getRelativePath());
        throw new CustomInternalServerError("File at relative path " + element.getRelativePath() + " not found. Aborting delivery.");
      }
    }

    String resourceUrl = collection.get(0).getRepositoryLocation();
    resourceUrl = resourceUrl.substring(0, resourceUrl.indexOf("/data/"));

    String resourceId = resourceUrl.substring(resourceUrl.lastIndexOf("/") + 1);

    Path rootDir = Paths.get(System.getProperty("java.io.tmpdir"), resourceId + "_bag_" + System.currentTimeMillis());
    Path zipDestination = Paths.get(rootDir.toString(), "../" + rootDir.getName(rootDir.getNameCount() - 1) + ".zip");

    try{
      BagBuilder builder = BagBuilder.create(rootDir);

      for(CollectionElement element : collection){
        FetchItem item = new FetchItem(URI.create(element.getRepositoryLocation() + element.getRelativePath()).toURL(), element.getContentLength(), Paths.get(rootDir.toAbsolutePath().toString(), element.getRelativePath()));
        Map<String, String> checksums = new HashMap<>();
        checksums.put("SHA1", element.getChecksum());
        builder.addFetchItem(item, checksums);
      }

      RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

      HttpHeaders headers = new HttpHeaders();
      headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
      HttpEntity<String> entity = new HttpEntity<String>(headers);

      //get all metadata resources
      ResponseEntity<DataResource> restResponse = restTemplate.exchange(URI.create(resourceUrl), HttpMethod.GET, entity, DataResource.class);

      
      //get all content information entries 
      //remove all elements not in the collection
      //serialize to contentInformation.xml
      
      
      //create all metadata entities
      DataResource resource = restResponse.getBody();
      Resource dataCiteResource = DataCiteMapper.dataResourceToDataciteResource(resource);
      ElementContainer dcContainer = DublinCoreMapper.dataResourceToDublinCoreContainer(resource);

      Path datacitePath = Paths.get(rootDir.toAbsolutePath().toString(), "metadata", "datacite.xml");
      Path dataResourcePath = Paths.get(rootDir.toAbsolutePath().toString(), "metadata", "dataResource.xml");
      Path dcPath = Paths.get(rootDir.toAbsolutePath().toString(), "metadata", "dc.xml");

      //check
      Files.createDirectories(datacitePath.getParent());

      //marshal datacite resource
      JAXBContext jaxbContext = JAXBContext.newInstance(Resource.class);
      Marshaller marshaller = jaxbContext.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      marshaller.marshal(dataCiteResource, new FileOutputStream(datacitePath.toFile()));

      //marshal dataresource 
      jaxbContext = JAXBContext.newInstance(DataResource.class);
      marshaller = jaxbContext.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      marshaller.marshal(resource, new FileOutputStream(dataResourcePath.toFile()));

      //marshal dc 
      jaxbContext = JAXBContext.newInstance(ElementContainer.class);
      marshaller = jaxbContext.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      marshaller.marshal(dcContainer, new FileOutputStream(dcPath.toFile()));

      //obtain data resource document and write it as datacite metadata (element.getRepositoryLocation() - '/data*'
      builder.addTagfile(datacitePath.toUri());
      builder.addTagfile(dataResourcePath.toUri());
      builder.addTagfile(dcPath.toUri());

      //adding metadata and write
      builder.addMetadata("External-Identifier", resourceId).
              addMetadata("Bagging-Date", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC).format(Instant.now())).
              addMetadata("External-Description", "BagIt export from KIT Data Manager 2.0").write();

      ZipUtils.zip(new File[]{rootDir.toFile()}, rootDir.toAbsolutePath().getParent().toString(), zipDestination.toFile());

      response.setContentType(mediaType.toString());
      response.setStatus(HttpServletResponse.SC_OK);

      FileUtils.copyFile(zipDestination.toFile(), response.getOutputStream());
    } catch(Exception e){
      LOGGER.error("Failed to create bag at " + rootDir, e);
      throw new CustomInternalServerError("Failed to create BagIt package.");
    } finally{
      try{
        System.out.println("DELETE ZIP ");
        Files.delete(zipDestination);
      } catch(IOException ex){
      }

      try{
        System.out.println("DELETE ROOT");
        Files.delete(rootDir);
      } catch(IOException ex){
      }
    }

    //create bag
    //write all elements as fetch files pointing to the KIT DM URLs (element.getRepositoryLocation() + relativePath)
    //write bag to temp folder
    //deliver bag from temp folder
  }

  @Override
  public boolean canProvide(String schema){
    LOGGER.trace("Calling canProvide({}).", schema);
    return "http".equals(schema) || "https".equals(schema) || "file".equals(schema);
  }

  @Override
  public boolean supportsMediaType(MediaType mediaType){
    LOGGER.trace("Calling supportsMediaType({}).", mediaType);
    if(mediaType == null){
      return false;
    }
    return BAGIT_MEDIA_TYPE.toString().equals(mediaType.toString());
  }

  @Override
  public MediaType[] getSupportedMediaTypes(){
    LOGGER.trace("Calling getSupportedMediaTypes().");
    return new MediaType[]{BAGIT_MEDIA_TYPE};
  }

}
