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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import gov.loc.repository.bagit.conformance.BagProfileChecker;
import gov.loc.repository.bagit.conformance.profile.BagInfoRequirement;
import gov.loc.repository.bagit.conformance.profile.BagitProfile;
import gov.loc.repository.bagit.conformance.profile.BagitProfileDeserializer;
import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.FetchItem;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.domain.Metadata;
import gov.loc.repository.bagit.domain.Version;
import gov.loc.repository.bagit.hash.StandardBagitAlgorithmNameToSupportedAlgorithmMapping;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import gov.loc.repository.bagit.reader.BagReader;
import gov.loc.repository.bagit.verify.BagVerifier;
import gov.loc.repository.bagit.verify.QuickVerifier;
import gov.loc.repository.bagit.writer.BagWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jejkal
 */
public class BagBuilder{

  /**
   * Default profile used if no other profile is provided.
   */
  public static String BAGIT_PROFILE_LOCATION = "https://raw.githubusercontent.com/RDAResearchDataRepositoryInteropWG/bagit-profiles/master/generic/0.1/profile.json";

  enum FILE_TYPE{
    PAYLOAD,
    TAGFILE,
    RDA_METADATA;
  }

  /**
   * The bag holding all information added by the builder.
   */
  private final Bag theBag;
  /**
   * The BagIt profile loaded from the profileLocation.
   */
  private BagitProfile profile;
  /**
   * Bag metadata holding all information stored in the bag's bag-info.txt
   */
  private final Metadata bagMetadata;
  /**
   * All payload manifests of the bag.
   */
  private final Set<Manifest> payloadManifests = new HashSet<>();
  /**
   * All tag manifests of the bag.
   */

  private final Set<Manifest> tagManifests = new HashSet<>();
  /**
   * All fetch items of the bag.
   */

  private List<FetchItem> fetchItems = null;
  /**
   * The URL to the used BagIt profile.
   */
  private String profileLocation = BAGIT_PROFILE_LOCATION;
  /**
   * The current bag size.
   */
  private long bagSize = 0;
  /**
   * The current size of all payload items.
   */
  private long payloadSize = 0;

  /**
   * Hidden default constructor used by {@link #create(java.nio.file.Path, java.lang.String)
   * }.
   *
   * @param rootDir The absolute path of the bag root dir.
   *
   * @throws Exception If any of the checksum algorithms required by the profile
   * is not supported or if the profile cannot be read.
   */
  BagBuilder(Path rootDir, String profileUrl) throws Exception{
    theBag = new Bag(new Version(0, 97));
    theBag.setRootDir(rootDir);
    theBag.setFileEncoding(Charset.forName("UTF-8"));
    bagMetadata = new Metadata();
    bagMetadata.add("BagIt-Profile-Identifier", profileLocation);
    theBag.setMetadata(bagMetadata);
    //load profile
    profile = parseBagitProfile(new URL(profileLocation).openStream());
    //build set of required payload manifests
    List<String> payloadMmanifestsRequired = profile.getManifestTypesRequired();
    payloadMmanifestsRequired.stream().map((required) -> new Manifest(StandardSupportedAlgorithms.valueOf(required.toUpperCase()))).map((manifestType) -> {
      manifestType.setFileToChecksumMap(new HashMap<>());
      return manifestType;
    }).forEachOrdered((manifestType) -> {
      payloadManifests.add(manifestType);
    });
    theBag.setPayLoadManifests(payloadManifests);

    //build set of required tag manifests
    List<String> tagManifestsRequired = profile.getTagManifestTypesRequired();
    tagManifestsRequired.stream().map((required) -> new Manifest(StandardSupportedAlgorithms.valueOf(required.toUpperCase()))).map((manifestType) -> {
      manifestType.setFileToChecksumMap(new HashMap<>());
      return manifestType;
    }).forEachOrdered((manifestType) -> {
      tagManifests.add(manifestType);
    });
    theBag.setTagManifests(tagManifests);

    if(profile.isFetchFileAllowed()){
      fetchItems = new ArrayList<>();
      //set fetch item list
      theBag.setItemsToFetch(fetchItems);
    }
  }

  /**
   * Hidden default constructor used by {@link #create(java.nio.file.Path) }.
   *
   * @param rootDir The absolute path of the bag root dir.
   *
   * @throws Exception If any of the checksum algorithms required by the profile
   * is not supported or if the profile cannot be read.
   */
  BagBuilder(Path rootDir) throws Exception{
    this(rootDir, BAGIT_PROFILE_LOCATION);
  }

  /**
   * Hidden constructor used by {@link #load(java.nio.file.Path) } taking an
   * existing bag as argument, e.g. for import or for validation purposes.
   *
   * @param existing The existing bag read, e.g. read from disk.
   *
   * @throws Exception If any of the checksum algorithms required by the profile
   * is not supported or if the profile cannot be read.
   */
  BagBuilder(Bag existing){
    this.theBag = new Bag(existing);
    bagMetadata = theBag.getMetadata();
    List<String> profileId = bagMetadata.get("BagIt-Profile-Identifier");
    if(profileId != null && !profileId.isEmpty()){
      profileLocation = profileId.get(0);
      // AnsiUtil.printInfo(MESSAGES.getString("using_profile_from_metadata"), profileLocation);
    } else{
      //  AnsiUtil.printInfo(MESSAGES.getString("using_default_profile"), profileLocation);
    }
  }

  /**
   * Create a new bag with the provided root dir using the default profile.
   *
   * @param rootDir The absolute path of the bag root dir.
   *
   * @return This BagBuilder instance.
   *
   * @throws Exception If any of the checksum algorithms required by the profile
   * is not supported or if the profile cannot be read.
   */
  public static BagBuilder create(Path rootDir) throws Exception{
    return new BagBuilder(rootDir);
  }

  /**
   * Create a new bag with the provided root dir using an alternate profile.
   *
   * @param rootDir The absolute path of the bag root dir.
   * @param profileUrl The URL pointing to a publicly available BagIt profile.
   *
   * @return This BagBuilder instance.
   *
   * @throws Exception If any of the checksum algorithms required by the profile
   * is not supported or if the profile cannot be read.
   */
  public static BagBuilder create(Path rootDir, String profileUrl) throws Exception{
    return new BagBuilder(rootDir, profileUrl);
  }

  /**
   * Load the bag located at the provided path.
   *
   * @param rootDir The absolute path for the bag root dir.
   *
   * @return This BagBuilder instance.
   *
   * @throws Exception if loading the bag fails for some reason, e.g. if the
   * used hash algorithm is not supported.
   */
  public static BagBuilder load(Path rootDir) throws Exception{
    return new BagBuilder(new BagReader(new StandardBagitAlgorithmNameToSupportedAlgorithmMapping()).read(rootDir));
  }

  /**
   * Add user-provided properties as metadata entries to the bag. The properties
   * object is expected to contain at least all mandatory properties required
   * according to the used BagIt profile. Only the property External-Identifier
   * is added by this tool. Furthermore, other properties can be added.
   *
   * @param properties The properties added as metadata into bag-info.txt
   *
   * @throws Exception If at least one mandatory property (according to the used
   * profile) is missing or if the property value does not match any element of
   * the list of acceptable value (according to the profile).
   */
  public void validateAndAddMetadataProperties(Properties properties) throws Exception{
    Set<Entry<String, BagInfoRequirement>> requirements = profile.getBagInfoRequirements().entrySet();

    for(Entry<String, BagInfoRequirement> requirement : requirements){
      String propValue = properties.getProperty(requirement.getKey());
      boolean elementAlreadyExists = false;
      if(requirement.getValue().isRequired()){
        final List<String> existingMetadata = theBag.getMetadata().get(requirement.getKey());
        if(propValue == null && (existingMetadata == null || existingMetadata.isEmpty())){
          throw new Exception("Mandatory metadata with key " + requirement.getKey() + " is missing.");
        } else{
          elementAlreadyExists = propValue == null && existingMetadata != null && !existingMetadata.isEmpty();
        }
      }

      List<String> acceptableValues = requirement.getValue().getAcceptableValues();
      if(acceptableValues != null && !acceptableValues.isEmpty()){
        if(!acceptableValues.contains(propValue)){
          throw new Exception("Invalid metadata value for key " + requirement.getKey() + ". Provided value was " + propValue + ", allowed values are " + acceptableValues.toString() + ".");
        }
      }

      //re-enable adding metadata as soon as bagit-java supports the 'repeatable' property
      //addMetadata(requirement.getKey(), propValue);
      if(propValue != null && !elementAlreadyExists){
        replaceMetadata(requirement.getKey(), propValue);
        properties.remove(requirement.getKey());
      } else{
        properties.remove(requirement.getKey());
      }
    }

    //add other user-provided properties not required by the profile
    Set<Object> additionalProps = properties.keySet();
    additionalProps.forEach((key) -> {
      addMetadata((String) key, (String) properties.getProperty((String) key));
    });
  }

  /**
   * Returns the list of tag payload types required by the used profile.
   *
   * @return The list of tag payload types.
   */
  public Set<String> getRequiredPayloadManifestTypes(){
    Set<String> manifestTypes = new HashSet<>();
    theBag.getPayLoadManifests().forEach((m) -> {
      manifestTypes.add(m.getAlgorithm().getMessageDigestName());
    });
    return manifestTypes;
  }

  /**
   * Returns the list of tag manifest types required by the used profile.
   *
   * @return The list of tag manifest types.
   */
  public Set<String> getRequiredTagManifestTypes(){
    Set<String> manifestTypes = new HashSet<>();
    theBag.getTagManifests().forEach((m) -> {
      manifestTypes.add(m.getAlgorithm().getMessageDigestName());
    });
    return manifestTypes;
  }

  /**
   * Add a metadata field and its value later written to bag-info.txt. This
   * method only adds metadata fields. If there is already a field for the
   * provided key, the new value will be added as second entry with the same
   * key. All added fields are stored later in bag-info.txt
   *
   * @param key The metadata key.
   * @param value The metadata value.
   *
   * @return This BagBuilder instance.
   */
  public BagBuilder addMetadata(String key, String value){
    return addOrReplaceMetadata(key, value, false);
  }

  /**
   * Add a metadata field and its value later written to bag-info.txt. This
   * method takes care, that there is only one entry with the provided key in
   * the bag metadata. If there is already a field for the provided key, the
   * existing element is replaced by the provided value.
   *
   * @param key The metadata key.
   * @param value The metadata value.
   *
   * @return This BagBuilder instance.
   */
  public BagBuilder replaceMetadata(String key, String value){
    return addOrReplaceMetadata(key, value, true);
  }

  /**
   * Add a new payload entry using the provided file URI. The file must be
   * located on the local hard disk within the bag data directory. If not, an
   * IOException is thrown.
   *
   * @param fileUri The payload URI which has to be within the bag root
   * directory.
   *
   * @return This BagBuilder instance.
   *
   * @throws IOException If fileUri is not located within the bag root
   * directory.
   */
  public BagBuilder addPayload(URI fileUri) throws IOException{
    return addFile(fileUri, FILE_TYPE.PAYLOAD);
  }

  /**
   * Add a new payload entry located a fileUri relative to rootPath. The file
   * will be copied to the bag payload directory to inDataLocation. If
   * inDataLocation is null, the path of fileUri relative to rootPath will be
   * used as payload path.
   *
   *
   * @param rootPath The root path where fileUri is located.
   * @param fileUri The absolute path of the payload.
   * @param inDataLocation The relative path within the bag payload directory,
   * if the relative path of fileUri within rootPath should not be used.
   *
   * @return This BagBuilder instance.
   *
   * @throws IOException if filePath does not exist, is not readable or if no
   * inDataLocation is specified and fileUri is not withing rootPath.
   */
  public BagBuilder addPayload(Path rootPath, URI fileUri, String inDataLocation) throws IOException{
    return addFile(rootPath, fileUri, inDataLocation, FILE_TYPE.PAYLOAD);
  }

  /**
   * Add a new tag file entry located a fileUri relative to rootPath. The file
   * will be copied to the bag root directory to inDataLocation. If
   * inDataLocation is null, the path of fileUri relative to rootPath will be
   * used as tag file path.
   *
   *
   * @param rootPath The root path where fileUri is located.
   * @param fileUri The absolute path of the tag file.
   * @param inDataLocation The relative path within the bag root directory, if
   * the relative path of fileUri within rootPath should not be used.
   *
   * @return This BagBuilder instance.
   *
   * @throws IOException if filePath does not exist, is not readable or if no
   * inDataLocation is specified and fileUri is not withing rootPath.
   */
  public BagBuilder addTagfile(Path rootPath, URI fileUri, String inDataLocation) throws IOException{
    return addFile(rootPath, fileUri, inDataLocation, FILE_TYPE.TAGFILE);
  }

  /**
   * Add a new tag file entry using the provided file URI. The file must be
   * located on the local hard disk within the bag root directory. If not, an
   * IOException is thrown.
   *
   * @param fileUri The tag file URI which has to be within the bag root
   * directory.
   *
   * @return This BagBuilder instance.
   *
   * @throws IOException If fileUri is not located within the bag root
   * directory.
   */
  public BagBuilder addTagfile(URI fileUri) throws IOException{
    return addFile(fileUri, FILE_TYPE.TAGFILE);
  }

  /**
   * Add a new fetch item to the bag. Fetch items are files which are not
   * contained in the bag but have to be downloaded by the bag receiver. The bag
   * only contains checksums and destination information of the file to fetch.
   * Fetching is only available for payload elements and must be allowed by the
   * BagIt profile associated to the bag. In order to create the checkums of the
   * fetch file, the file must be opened and read. If you want to avoid reading
   * the fetch files, you may use {@link #addFetchItem(gov.loc.repository.bagit.domain.FetchItem, java.util.Map)
   * } and provide all requested checksums manually. You can obtain the list of
   * required checksums by calling {@link #getRequiredPayloadManifestTypes()} or {@link #getRequiredTagManifestTypes()
   * }.
   *
   * @param item The fetch item to add.
   *
   * @return This BagBuilder instance.
   *
   * @throws Exception If fetching items is not allowed by the used profile or
   * if the fetch items input stream cannot be opened.
   */
  public BagBuilder addFetchItem(FetchItem item) throws Exception{
    if(fetchItems == null){
      throw new Exception("Fetching is not allowed by the used profile.");
    }
    payloadSize += item.getLength();
    Path thePath = theBag.getRootDir().resolve(item.getPath());
    fetchItems.add(new FetchItem(item.getUrl(), item.getLength(), thePath));

    generateChecksums(thePath, item.getUrl().openConnection().getInputStream(), FILE_TYPE.PAYLOAD);
    return this;
  }

  /**
   * Add a new fetch item to the bag. Fetch items are files which are not
   * contained in the bag but have to be downloaded by the bag receiver. The bag
   * only contains checksums and destination information of the file to fetch.
   * Fetching is only available for payload elements and must be allowed by the
   * BagIt profile associated to the bag. In order to avoid reading the fetch
   * file from its URL, the map of checksums can be provided manually.
   *
   * @param item The fetch item to add.
   * @param checksums The map of all checksums required by the bag.
   *
   * @return This BagBuilder instance.
   *
   * @throws Exception If fetching items is not allowed by the used profile or
   * if the fetch items input stream cannot be opened.
   */
  public BagBuilder addFetchItem(FetchItem item, Map<String, String> checksums) throws Exception{
    if(fetchItems == null){
      throw new Exception("Fetching is not allowed by the used profile.");
    }
    payloadSize += item.getLength();
    Path thePath = theBag.getRootDir().resolve(item.getPath());

    fetchItems.add(new FetchItem(item.getUrl(), item.getLength(), thePath));

    theBag.getPayLoadManifests().forEach((manifest) -> {
      String checksum = checksums.get(manifest.getAlgorithm().getMessageDigestName());
      //  AnsiUtil.printInfo(MESSAGES.getString("adding_user_provided_checksum"), thePath.toString(), checksum, manifest.getAlgorithm().getMessageDigestName());
      if(checksum != null){
        manifest.getFileToChecksumMap().put(thePath, checksum);
      }
    });

    return this;
  }

  /**
   * Validate the bag according to the specified profile.
   *
   * @throws Exception If the bag is not compliant to the specified profile.
   */
  public void validateProfileConformance() throws Exception{
    //AnsiUtil.printInfo(MESSAGES.getString("performing_profile_check"), profileLocation);
    BagProfileChecker.bagConformsToProfile(new URL(profileLocation).openStream(), theBag);
    // AnsiUtil.printInfo(MESSAGES.getString("profile_check_successful"), profileLocation);
  }

  /**
   * Validate all checksums in all manifests of the bag. This method will try to
   * use the QuickVerifier feature of the underlying bagit library. If fetching
   * files is used (fetch.txt is present), quick verification is not possible.
   * If you want to verify bags with fetch items, you should download the items
   * before validating the checksums and state this by providing TRUE for
   * argument 'fetchFilesDownloaded'. If 'fetchFilesDownloaded' is provided but
   * any fetch file is not downloaded yet, validation will fail with an
   * exception.
   *
   * @param fetchFilesDownloaded TRUE if all files in fetch.txt are downloaded
   * to the local bag payload directory.
   *
   * @throws Exception If any of the checksums in any bag manifest is not valid.
   */
  public void validateChecksums(boolean fetchFilesDownloaded) throws Exception{
    // AnsiUtil.printInfo(MESSAGES.getString("checking_quick_verify_support"));
    if(BagVerifier.canQuickVerify(theBag)){
      // AnsiUtil.printInfo(MESSAGES.getString("performing_quick_verify"));
      QuickVerifier.quicklyVerify(theBag);
    } else{
      if(fetchFilesDownloaded){
        //  AnsiUtil.printWarning(MESSAGES.getString("quick_verify_not_supported_but_files_fetched"));
        new BagVerifier(new StandardBagitAlgorithmNameToSupportedAlgorithmMapping()).isValid(theBag, true);
      } else{
        // AnsiUtil.printWarning(MESSAGES.getString("quick_verify_not_supported"));
      }
    }
    //AnsiUtil.printInfo(MESSAGES.getString("verification_successful"));
  }

  /**
   * Write the bag to its root directory. Typically, most of the content should
   * be already located relative to the bag root directory. Files that are not
   * already there are copied to the according sub path. Furthermore, all
   * missing metadata files are created and written to disk.
   *
   * @throws IOException If writing the bag fails, e.g. due to missing write
   * permissions to the bag root directory.
   * @throws NoSuchAlgorithmException If creating any checksum fails due to an
   * unsupported checksum algorithm used for any of the manifests.
   */
  public void write() throws IOException, NoSuchAlgorithmException{
    write(theBag.getRootDir());
  }

  /**
   * Write the bag to the provided destination. Files that are not already there
   * are copied. Furthermore, all missing metadata files are created and written
   * to disk.
   *
   * @param destination The bag destination directory.
   *
   * @throws IOException If writing the bag fails, e.g. due to missing write
   * permissions to destination.
   * @throws NoSuchAlgorithmException If creating any checksum fails due to an
   * unsupported checksum algorithm used for any of the manifests.
   */
  public void write(Path destination) throws IOException, NoSuchAlgorithmException{
    BagWriter.write(getBag(), destination.toAbsolutePath());
  }

  /**
   * Returns the current bag created/read by this builder.
   *
   * @return The current bag.
   */
  public Bag getBag(){
    if(bagMetadata.contains("Bag-Size")){
      bagMetadata.remove("Bag-Size");
    }
    bagMetadata.add("Bag-Size", FileUtils.byteCountToDisplaySize(bagSize));

    return theBag;
  }

  /**
   * Get the current bag size in bytes including payload, tag and fetch files.
   *
   * @return The bag size.
   */
  public long getBagSize(){
    return bagSize;
  }

  /**
   * Get the current payload size in bytes including only payload files.
   *
   * @return The bag size.
   */
  public long getPayloadSize(){
    return payloadSize;
  }

  /**
   * Parse a BagIt profile from an InputStream.
   *
   * @param jsonProfile The input stream.
   *
   * @return The BagItProfile.
   *
   * @throws IOException if the profile cannot be read or has no valid format.
   */
  private BagitProfile parseBagitProfile(final InputStream jsonProfile) throws IOException{
    final ObjectMapper mapper = new ObjectMapper();
    final SimpleModule module = new SimpleModule();
    module.addDeserializer(BagitProfile.class, new BagitProfileDeserializer());
    mapper.registerModule(module);

    return mapper.readValue(jsonProfile, BagitProfile.class);
  }

  /**
   * Add file helper for external files not located relative to bag root.
   *
   * @param rootPath The root path of the file to add.
   * @param fileUri The absolute file URI starting with rootPath.
   * @param inBagLocation The relative location of the file inside the bag.
   * According to the provided type, the file will be placed either at the bag
   * root (TAGFILE), relative to the payload data directory (PAYLOAD) or
   * relative to the metadata directory (RDA_METADATA).
   * @param type The file type defining the file location within the bag.
   *
   * @return This BagBuilder instance.
   *
   * @throws IOException If filePath is not accessible or if inBagLocation is
   * not provided and fileUri is not relative to rootPath.
   */
  private BagBuilder addFile(Path rootPath, URI fileUri, String inBagLocation, FILE_TYPE type) throws IOException{
    Path filePath = Paths.get(fileUri);

    if(inBagLocation != null && !filePath.startsWith(rootPath)){
      throw new IOException("File " + filePath.toString() + " is not located relative to bag root " + rootPath.toString() + ".");
    }
    if(!Files.exists(filePath)){
      throw new IOException("File " + filePath.toString() + "does not exist.");
    }
    if(!Files.isReadable(filePath)){
      throw new IOException("File " + filePath.toString() + " is not readable.");
    }

    Path destination;
    String relativeFilePath;
    if(inBagLocation == null){
      //obtain relative potion of filePath compared to rootPath
      relativeFilePath = rootPath.relativize(filePath).toString();
    } else{
      //use provided inDataPath as relative file path
      relativeFilePath = inBagLocation;
    }

    switch(type){
      case PAYLOAD:
        destination = Paths.get(theBag.getRootDir().toString(), "data").resolve(relativeFilePath);
        break;
      case RDA_METADATA:
        destination = Paths.get(theBag.getRootDir().toString(), "metadata").resolve(relativeFilePath);
        break;
      default://other tag files
        destination = Paths.get(theBag.getRootDir().toString()).resolve(relativeFilePath);
        break;
    }

    //create folder structure before copy operation
    if(!Files.exists(destination)){
      Files.createDirectories(destination);
    }
    //copy file
    Files.copy(filePath, destination, StandardCopyOption.REPLACE_EXISTING);

    addFile(destination.toUri(), type);
    return this;
  }

  /**
   * Add file helper for files that are already located relative to bag root.
   * This method creates the checksums of the file and adds the file path
   * relative to the bag root directory into the manifest according to the
   * provided type, which is either the tag-manifest (type TAGFILE or
   * RDA_METADATA) or the payload manifest (type PAYLOAD).
   *
   * @param fileUri The absolute file Uri relative to the bag root directory.
   * @param type The file type defining the manifest the file will be added to.
   *
   * @return This BagBuilder instance.
   *
   * @throws IOException If creating any checksum of fileUri fails.
   */
  private BagBuilder addFile(URI fileUri, FILE_TYPE type) throws IOException{
    Path filePath = Paths.get(fileUri);
    long fileSize = FileUtils.sizeOf(filePath.toFile());
    bagSize += fileSize;
    if(FILE_TYPE.PAYLOAD.equals(type)){
      payloadSize += fileSize;
    }

    if(filePath.toAbsolutePath().startsWith(theBag.getRootDir().toAbsolutePath())){
      //file relative to root: hash separately
      generateChecksums(filePath, Files.newInputStream(filePath), type);
    } else{
      throw new IOException("File path " + filePath + " is not relative to bag root path " + theBag.getRootDir() + ".");
    }

    return this;
  }

  /**
   * Adds or replaces a single metadata field and its value later written to
   * bag-info.txt.
   *
   * @param key The metadata key.
   * @param value The metadata value.
   * @param replaceIfExists If TRUE, existing metadata with the same key is
   * replaced. Otherwise, a new entry with the same key is added.
   *
   * @return This BagBuilder instance.
   */
  private BagBuilder addOrReplaceMetadata(String key, String value, boolean replaceIfExists){
    if(replaceIfExists && bagMetadata.contains(key)){
      bagMetadata.remove(key);
    }
    bagMetadata.add(key, value);
    return this;
  }

  /**
   * Generate and add all checksums required by the used BagIt profile.
   * Depending on the provide type, the checksum(s) are added either to the
   * payload manifest(s) or the tagfile manifest(s). In order to generate the
   * checksum(s), the entire file has to be read once using the provided input
   * stream.
   *
   * @param filePath The absolute file path relative to the bag root.
   * @param stream The input stream which is either the stream to filePath or a
   * stream to an external resource when adding fetch files.
   * @param type The file type defining to which manifest the checksums are
   * written, which is either the tag-manifest (type TAGFILE or RDA_METADATA) or
   * the payload manifest (type PAYLOAD).
   *
   * @throws IOException if nothing can be read from the input stream.
   */
  private void generateChecksums(Path filePath, InputStream stream, FILE_TYPE type) throws IOException{
    int read;
    byte[] data = new byte[100 * (int) FileUtils.ONE_KB];
    Map<String, MessageDigest> digestMap = new HashMap<>();

    //AnsiUtil.printInfo(MESSAGES.getString("generating_checksums"), filePath.toString());
    switch(type){
      case PAYLOAD: {
        theBag.getPayLoadManifests().stream().map((manifest) -> manifest.getAlgorithm().getMessageDigestName()).forEachOrdered((digestName) -> {
          digestMap.put(digestName, DigestUtils.getDigest(digestName));
        });
        break;
      }
      default: {
        theBag.getTagManifests().stream().map((manifest) -> manifest.getAlgorithm().getMessageDigestName()).forEachOrdered((digestName) -> {
          digestMap.put(digestName, DigestUtils.getDigest(digestName));
        });
        break;
      }
    }

    // AnsiUtil.printInfo(MESSAGES.getString("creating_checksums_from_stream"), Integer.toString(digestMap.size()));
    while((read = stream.read(data)) > -1){
      for(Entry<String, MessageDigest> digest : digestMap.entrySet()){
        digest.getValue().update(data, 0, read);
      }
      data = new byte[100 * (int) FileUtils.ONE_KB];
    }

    switch(type){
      case PAYLOAD: {
        theBag.getPayLoadManifests().forEach((manifest) -> {
          final String digestName = manifest.getAlgorithm().getMessageDigestName();
          final MessageDigest digest = digestMap.get(manifest.getAlgorithm().getMessageDigestName());
          final String checksum = Hex.encodeHexString(digest.digest());
          //    AnsiUtil.printInfo(MESSAGES.getString("adding_checksum_to_manifest"), digestName, checksum, "payload");
          manifest.getFileToChecksumMap().put(filePath, checksum);
        });
        break;
      }
      default: {
        theBag.getTagManifests().forEach((manifest) -> {
          final String digestName = manifest.getAlgorithm().getMessageDigestName();
          final MessageDigest digest = digestMap.get(manifest.getAlgorithm().getMessageDigestName());
          final String checksum = Hex.encodeHexString(digest.digest());
          //   AnsiUtil.printInfo(MESSAGES.getString("adding_checksum_to_manifest"), digestName, checksum, "tag");
          manifest.getFileToChecksumMap().put(filePath, checksum);
        });
      }
    }
  }

}
