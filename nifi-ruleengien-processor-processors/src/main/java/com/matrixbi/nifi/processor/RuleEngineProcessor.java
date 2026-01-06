/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.matrixbi.nifi.processor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.kie.api.definition.type.Description;

import com.matrixbi.objects.JsonBusinessObjects;
import com.matrixbi.utils.RuleEngine;

import java.security.MessageDigest;
import java.util.Base64;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;


import org.kie.api.KieServices;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.Message;
import org.kie.api.io.ResourceType;

import org.apache.nifi.components.Validator;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;

import java.io.File;
import java.util.List;

@SideEffectFree
@Tags({"Rule Engine","Processor","Drools","drl","MatrixBI"})
@CapabilityDescription("Rule engine for nifi")
@Description("This is rule engien")
public class RuleEngineProcessor extends AbstractProcessor {

//     public static final PropertyDescriptor DRL_PATH = new PropertyDescriptor
//         .Builder().name("DRL file path")
//         .displayName("DRL file path")
//         .description("File ends with .drl or .xls that contines drools rules")
//         .required(true)
//         .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//         .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
//         .build();

    public static final PropertyDescriptor DRL_CONTENT = new PropertyDescriptor
        .Builder().name("DRL Content")
        .displayName("DRL Rule Content")
        .description("Drools DRL rule content")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .addValidator(new DrlSyntaxValidator())  // 使用自定义验证器
        .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Success relationship")
        .build();

    public static final Relationship FAILD = new Relationship.Builder()
        .name("failed")
        .description("Failed relationship")
        .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();

    private static HashMap<String,RuleEngine> ruleEngineServices = new HashMap<>();

    private ComponentLog log;


    private static RuleEngine getRuleEngineService_old(String filepath) {
    	if(!ruleEngineServices.containsKey(filepath))
    		ruleEngineServices.put(filepath, RuleEngine.createSession(filepath));

    	return ruleEngineServices.get(filepath);
    }

    // 自定义DRL语法验证器
    public static class DrlSyntaxValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (input == null || input.trim().isEmpty()) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("DRL内容不能为空")
                        .build();
            }

            try {
                CheckResult result = checkDrlContent(input);
                if (result.isValid()) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(true)
                            .explanation("DRL语法正确")
                            .build();
                } else {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("DRL语法错误: " + result.getErrorMessages())
                            .build();
                }
            } catch (Exception e) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("验证过程中发生异常: " + e.getMessage())
                        .build();
            }
        }

        private CheckResult checkDrlContent(String drlContent) {
            /* 利用Drools的编译机制，对DRL文件做一次离线编译，用来检查语法/规则是否合法。
               它不执行规则，只做“能不能被Drools成功编译”的校验。
               Drools 没有单独的“语法校验器”，只要规则能成功build，就说明语法+规则结构是正确的。

               DRL 文件
                 → 加入 KieFileSystem
                 → 调用 KieBuilder.buildAll()
                 → 查看编译错误
            */
            // 获取Drools核心入口
            KieServices ks = KieServices.Factory.get();
            // 创建虚拟规则文件系统
            KieFileSystem kfs = ks.newKieFileSystem();

            // 直接将DRL字符串内容写入虚拟文件
            kfs.write("src/main/resources/rules.drl", ks.getResources().newByteArrayResource(drlContent.getBytes()).setResourceType(ResourceType.DRL));

            // 编译规则
            KieBuilder kieBuilder = ks.newKieBuilder(kfs).buildAll();
            List<Message> messages = kieBuilder.getResults().getMessages();

            List<String> errors = new ArrayList<>();
            for (Message message : messages) {
                errors.add(String.format("行%d列%d: %s",
                    message.getLine(), message.getColumn(), message.getText()));
            }

            return new CheckResult(messages.isEmpty(), errors);
        }
    }

    // 验证结果封装类
    public static class CheckResult {
        private final boolean valid;
        private final List<String> errorMessages;

        public CheckResult(boolean valid, List<String> errorMessages) {
            this.valid = valid;
            this.errorMessages = errorMessages != null ? errorMessages : new ArrayList<>();
        }

        public boolean isValid() {
            return valid;
        }

        public List<String> getErrorMessages() {
            return errorMessages;
        }

        public String getErrorMessagesAsString() {
            return String.join("; ", errorMessages);
        }
    }

    // 生成内容哈希（MD5/SHA-256）
    private static String generateHash(String content) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(content.getBytes("UTF-8"));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            // 如果哈希失败，使用内容本身作为键（注意内容太长的情况）
            return content.length() > 100 ? content.substring(0, 100) : content;
        }
    }

    // 从字符串内容创建RuleEngine（需要RuleEngine支持）
    private static RuleEngine createEngineFromContent(String drlContent) {
        // 方法二：创建临时文件
        try {
            Path tempFile = Files.createTempFile("rules_", ".drl");
            Files.write(tempFile, drlContent.getBytes());
            RuleEngine engine = RuleEngine.createSession(tempFile.toString());
            // 可以选择删除临时文件，或让RuleEngine读取后删除
            return engine;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp DRL file", e);
        }
    }

    private static RuleEngine getRuleEngineService(String drlContent) {
        // 1. 生成DRL内容的哈希作为唯一标识
        String contentHash = generateHash(drlContent);

        // 2. 检查缓存
        if(!ruleEngineServices.containsKey(contentHash)) {
            // 3. 使用内容创建RuleEngine（假设RuleEngine支持从字符串创建）
            RuleEngine engine = createEngineFromContent(drlContent);
            ruleEngineServices.put(contentHash, engine);
        }

        return ruleEngineServices.get(contentHash);
    }
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	log = getLogger();
    	log.debug("Init MatrixBI's RuleEngineProcesor");

    	final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
	        // descriptors.add(DRL_PATH);
	        descriptors.add(DRL_CONTENT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
	        relationships.add(SUCCESS);
	        relationships.add(FAILD);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        
        
        if ( flowFile == null ) {
            return;
        }
       
              
        final AtomicReference<JsonBusinessObjects> value = new AtomicReference<>();
        
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream flowfileInputStream) throws IOException {
                try{
                    InputStreamReader flowfileInputStreamReader = new InputStreamReader(flowfileInputStream);
                    JsonBusinessObjects jsonBusinessObjects = new JsonBusinessObjects(flowfileInputStreamReader);
                    
                    // String drl_path = context.getProperty(DRL_PATH).getValue();
                    String drl_content = context.getProperty(DRL_CONTENT).getValue();
                    while(jsonBusinessObjects.hasNext()) {
//                     	getRuleEngineService(drl_path).execute(jsonBusinessObjects.next());

                    	// 使用DRL内容获取RuleEngine
                    	getRuleEngineService(drl_content).execute(jsonBusinessObjects.next());
                    }
                    
                    value.set(jsonBusinessObjects);
                }catch(Exception ex){
                    log.error("Failed to read json string", ex);
                }
            }
        });

        // Write the results to an attribute
        JsonBusinessObjects results = value.get();
        
        if(results==null)
        {
        	log.error("Failed to get results");
        	session.transfer(flowFile, FAILD);	
        	return;
        }

        
        // if changed
        if(results.hasChanged()) {
	        flowFile = session.write(flowFile, new OutputStreamCallback() {
	            @Override
	            public void process(OutputStream out) throws IOException {
	                out.write(value.get().getJson().getBytes());
	            }
	        });
        }
        
        session.transfer(flowFile, SUCCESS);
        
    }
    

    @OnStopped
    public void onStopped() {
        bufferQueue.clear();
    }
}
